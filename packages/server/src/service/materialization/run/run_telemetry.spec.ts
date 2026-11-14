// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { MaterializationEligibilityError } from "../../../errors";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import type { BuildInstruction, ManifestEntry } from "../types";
import { errorCodeOf } from "./error_code";
import { BilledReadNotCapturedError } from "./materialization_build_session";
import {
   buildKindOf,
   buildKindsByAddress,
   failureClassOf,
   RUN_TRIGGERS,
   RunTelemetry,
   runTriggerOf,
   withRunTelemetry,
} from "./run_telemetry";

type LogCall = {
   level: string;
   message: string;
   meta: Record<string, unknown>;
};

/** Capture every logger call, at every level, for the test's duration. */
function captureLogs(): { logs: LogCall[]; restore: () => void } {
   const logs: LogCall[] = [];
   const stubs = (["debug", "info", "warn", "error"] as const).map((level) =>
      sinon.stub(logger, level).callsFake(((
         message: unknown,
         meta?: unknown,
      ) => {
         logs.push({
            level,
            message: String(message),
            meta: (meta ?? {}) as Record<string, unknown>,
         });
         return logger;
      }) as never),
   );
   return { logs, restore: () => stubs.forEach((s) => s.restore()) };
}

describe("run telemetry vocabulary", () => {
   it("names a run's trigger from the scheduler, the host, a publish, or a request", () => {
      expect([...RUN_TRIGGERS]).toEqual([
         "schedule",
         "api",
         "auto",
         "orchestrator",
      ]);
      expect(runTriggerOf({ trigger: "SCHEDULER", orchestrated: true })).toBe(
         "schedule",
      );
      expect(
         runTriggerOf({
            trigger: "ON_DEMAND",
            runContextTrigger: "scheduler",
            orchestrated: true,
         }),
      ).toBe("schedule");
      expect(runTriggerOf({ trigger: "ON_DEMAND", orchestrated: true })).toBe(
         "orchestrator",
      );
      expect(
         runTriggerOf({
            trigger: "ON_DEMAND",
            runContextTrigger: "publish",
            orchestrated: false,
         }),
      ).toBe("auto");
      expect(runTriggerOf({ trigger: "ON_DEMAND", orchestrated: false })).toBe(
         "api",
      );
   });

   it("names a build's kind from the plan that made its source", () => {
      const compiled = {
         preaggregatePlans: { "r@m": {} as never },
         materializePlans: {
            "m@m": {} as never,
            "p@m": { pair: {} } as never,
         },
      };
      expect(buildKindOf(compiled, "r@m")).toBe("rollup");
      expect(buildKindOf(compiled, "m@m")).toBe("materialize");
      expect(buildKindOf(compiled, "p@m")).toBe("pair");
      expect(
         buildKindOf(
            compiled,
            "orders__by_day__view@file:///m.malloy.persist-view.malloy",
         ),
      ).toBe("view");
      expect(buildKindOf(compiled, "orders@file:///m.malloy")).toBe("persist");
      expect(buildKindOf(compiled, undefined)).toBe("persist");
      expect(
         buildKindsByAddress(compiled, {
            sources: {
               "r@m": { sourceEntityId: "addr-r" },
               "o@m": { sourceEntityId: "addr-o" },
            } as never,
         }),
      ).toEqual(
         new Map([
            ["addr-r", "rollup"],
            ["addr-o", "persist"],
         ]),
      );
   });

   it("maps BuildRetryable to the failure class, a billed read first", () => {
      expect(failureClassOf(new Error("x"), false)).toBe("deterministic");
      expect(failureClassOf(new Error("x"), true)).toBe("transient");
      expect(failureClassOf(new Error("x"), undefined)).toBe("unclassified");
      const billed = new Error("wrapped", {
         cause: new BilledReadNotCapturedError("lost"),
      });
      expect(failureClassOf(billed, true)).toBe("billed_read_lost");
      expect(
         failureClassOf(new BilledReadNotCapturedError("lost"), false),
      ).toBe("billed_read_lost");
   });

   it("reduces an error to a code and never to its message", () => {
      expect(
         errorCodeOf(
            new MaterializationEligibilityError({
               message: "reads given ORG = 42",
               reason: "given_in_persisted_query",
            }),
         ),
      ).toBe("given_in_persisted_query");
      const driver = Object.assign(
         new Error(
            "relation \"secret\" does not exist: SELECT 'v' FROM secret",
         ),
         { code: "42P01" },
      );
      expect(errorCodeOf(driver)).toBe("42P01");
      expect(errorCodeOf(new TypeError("SELECT 1"))).toBe("TypeError");
      // A code that is not a short identifier is not a code.
      const odd = Object.assign(new Error("m"), { code: "SELECT * FROM t" });
      expect(errorCodeOf(odd)).toBe("Error");
      expect(errorCodeOf("a string thrown")).toBe("Error");
   });
});

describe("run telemetry through the build loop", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;
   let captured: ReturnType<typeof captureLogs>;

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      captured = captureLogs();
   });

   afterEach(async () => {
      captured.restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const instruction = (id: string, table: string): BuildInstruction => ({
      sourceEntityId: id,
      materializedTableId: `mt-${table}`,
      physicalTableName: table,
      realization: "COPY",
   });

   /**
    * Two colocated `#@ persist` sources, built by `buildOneSource`; the first
    * gated by `#(authorize)` when `gateFirst`, so the eligibility gate
    * refuses it.
    */
   async function runLoop(
      buildOneSource: sinon.SinonStub,
      opts: {
         mode?: "auto" | "orchestrated";
         abort?: AbortController;
         gateFirst?: boolean;
      } = {},
   ) {
      const connection = {
         runSQL: sinon.stub().resolves(),
      } as unknown as MalloyConnection;
      const compiled = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "a1aaaaaaaaaaaaaa",
               ...(opts.gateFirst
                  ? { sourceDef: { blockNotes: ["#(authorize) true"] } }
                  : {}),
            }),
            s2: fakeSource({ name: "s2", sourceEntityId: "a2bbbbbbbbbbbbbb" }),
         },
         [["s1"], ["s2"]],
         new Map([["duckdb", connection]]),
      );
      (ctx.service as unknown as { buildOneSource: unknown }).buildOneSource =
         buildOneSource;
      const telemetry = new RunTelemetry(
         "run-1",
         opts.mode ?? "orchestrated",
         "pkg",
      );
      const result = await withRunTelemetry(telemetry, () =>
         (
            ctx.service as unknown as {
               executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
            }
         )
            .executeInstructedBuild(
               compiled,
               {
                  getApiConnection: () => {
                     throw new Error("no config");
                  },
                  getEnvironmentPath: () => "/tmp/env",
               },
               [
                  instruction("a1aaaaaaaaaaaaaa", "s1_v1"),
                  instruction("a2bbbbbbbbbbbbbb", "s2_v1"),
               ],
               {},
               (opts.abort ?? new AbortController()).signal,
               false,
            )
            .then(
               (value) => ({ value }),
               (error: unknown) => ({ error }),
            ),
      );
      return { telemetry, result };
   }

   const entryFor = (
      id: string,
      extra: Partial<ManifestEntry> = {},
   ): ManifestEntry => ({
      sourceEntityId: id,
      physicalTableName: `${id}_t`,
      connectionName: "duckdb",
      buildDurationMs: 250,
      rowCount: 7,
      queryCostBytes: 3e6,
      ...extra,
   });

   it("counts and logs each built source once, with its duration and bytes", async () => {
      const build = sinon
         .stub()
         .callsFake(async (_s, i: BuildInstruction) =>
            entryFor(i.sourceEntityId),
         );
      const { telemetry } = await runLoop(build, { mode: "auto" });

      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            kind: "persist",
            tier: "colocated",
            outcome: "built",
            failure_class: "none",
         }),
      ).toBe(2);
      expect(
         await harness.collectHistogram(
            "publisher_materialization_build_duration_ms",
            { kind: "persist", tier: "colocated", method: "full" },
         ),
      ).toMatchObject({ count: 2, sum: 500 });
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_read_bytes",
               { tier: "colocated" },
            )
         ).count,
      ).toBe(2);
      expect(telemetry.tally).toMatchObject({ built: 2, failed: 0 });

      const finished = captured.logs.filter(
         (l) => l.message === "materialization.build.finished",
      );
      expect(finished).toHaveLength(2);
      expect(finished[0]).toMatchObject({
         level: "info",
         meta: {
            runId: "run-1",
            packageName: "pkg",
            sourceEntityId: "a1aaaaaaaaaaaaaa",
            sourceName: "s1",
            kind: "persist",
            tier: "colocated",
            method: "full",
            durationMs: 250,
            rows: 7,
            bytesRead: 3e6,
            // An auto-run made the name.
            physicalTable: "s1_v1",
         },
      });
   });

   it("leaves a host-assigned table name off the event, and does not time a skipped refresh", async () => {
      const build = sinon.stub().callsFake(async (_s, i: BuildInstruction) =>
         entryFor(i.sourceEntityId, {
            buildDurationMs: null,
            refresh: "none",
            queryCostBytes: null,
         }),
      );
      await runLoop(build, { mode: "orchestrated" });

      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_duration_ms",
            )
         ).count,
      ).toBe(0);
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_read_bytes",
            )
         ).count,
      ).toBe(0);
      const [finished] = captured.logs.filter(
         (l) => l.message === "materialization.build.finished",
      );
      expect(finished.meta).not.toHaveProperty("physicalTable");
      expect(finished.meta).not.toHaveProperty("durationMs");
      expect(finished.meta).not.toHaveProperty("bytesRead");
   });

   it("counts a failed source once, by its class, and logs its code and never its message", async () => {
      const message =
         "Permission denied: INSERT INTO t SELECT 'PLANTED_VALUE' FROM x";
      const build = sinon.stub().callsFake(async (_s, i: BuildInstruction) => {
         if (i.sourceEntityId === "a1aaaaaaaaaaaaaa") throw new Error(message);
         return entryFor(i.sourceEntityId);
      });
      const { telemetry } = await runLoop(build);

      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            outcome: "failed",
            failure_class: "deterministic",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_materialization_build_total"),
      ).toBe(2);
      // The failed build ran, so it is timed too.
      expect(
         (
            await harness.collectHistogram(
               "publisher_materialization_build_duration_ms",
            )
         ).count,
      ).toBe(2);
      expect(telemetry.tally).toMatchObject({ built: 1, failed: 1 });

      const failed = captured.logs.filter(
         (l) => l.message === "materialization.build.failed",
      );
      expect(failed).toHaveLength(1);
      expect(failed[0]).toMatchObject({
         level: "warn",
         meta: {
            runId: "run-1",
            outcome: "failed",
            failureClass: "deterministic",
            code: "Error",
            method: "full",
         },
      });
      for (const call of captured.logs) {
         const text = JSON.stringify([call.message, call.meta]);
         expect(text).not.toContain("PLANTED_VALUE");
         expect(text).not.toContain("INSERT INTO");
      }
   });

   it("counts a billed read lost once, however the strategy wrapped it", async () => {
      const build = sinon.stub().callsFake(async (_s, i: BuildInstruction) => {
         if (i.sourceEntityId === "a2bbbbbbbbbbbbbb") {
            throw new Error("Failed to materialize", {
               cause: new BilledReadNotCapturedError("read billed, rows lost"),
            });
         }
         return entryFor(i.sourceEntityId);
      });
      await runLoop(build);

      const points = await harness.collectAttributes(
         "publisher_materialization_build_total",
      );
      expect(points).toContainEqual({
         kind: "persist",
         tier: "colocated",
         outcome: "failed",
         failure_class: "billed_read_lost",
      });
      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            outcome: "failed",
         }),
      ).toBe(1);
      const [failed] = captured.logs.filter(
         (l) => l.message === "materialization.build.failed",
      );
      expect(failed.level).toBe("warn");
   });

   it("logs an unclassified failure as an error", async () => {
      const build = sinon.stub().rejects(new Error("something unrecognized"));
      const { result } = await runLoop(build);

      // Every source failed, so the run fails as a whole.
      expect(result).toHaveProperty("error");
      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            outcome: "failed",
            failure_class: "unclassified",
         }),
      ).toBe(2);
      expect(
         captured.logs
            .filter((l) => l.message === "materialization.build.failed")
            .map((l) => l.level),
      ).toEqual(["error", "error"]);
   });

   it("counts a source cut short by the run's cancellation as cancelled, not failed", async () => {
      const abort = new AbortController();
      const build = sinon.stub().callsFake(async () => {
         abort.abort();
         throw new Error("operation was aborted");
      });
      const { telemetry, result } = await runLoop(build, { abort });

      expect(result).toHaveProperty("error");
      // The second source never starts: the loop stops at the cancellation.
      expect(build.callCount).toBe(1);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_build_total",
         ),
      ).toEqual([
         {
            kind: "persist",
            tier: "colocated",
            outcome: "cancelled",
            failure_class: "none",
         },
      ]);
      expect(telemetry.tally).toMatchObject({ cancelled: 1, failed: 0 });
      const [event] = captured.logs.filter(
         (l) => l.message === "materialization.build.failed",
      );
      expect(event).toMatchObject({
         level: "info",
         meta: { outcome: "cancelled", failureClass: "none" },
      });
   });

   it("counts a source the eligibility gate refuses once, as refused, and does not build it", async () => {
      const build = sinon
         .stub()
         .callsFake(async (_s, i: BuildInstruction) =>
            entryFor(i.sourceEntityId),
         );
      const { telemetry } = await runLoop(build, { gateFirst: true });

      expect(build.callCount).toBe(1);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_build_total",
         ),
      ).toEqual(
         expect.arrayContaining([
            {
               kind: "persist",
               tier: "colocated",
               outcome: "refused",
               failure_class: "none",
            },
            {
               kind: "persist",
               tier: "colocated",
               outcome: "built",
               failure_class: "none",
            },
         ]),
      );
      expect(
         await harness.collectCounter("publisher_materialization_build_total"),
      ).toBe(2);
      expect(telemetry.tally).toMatchObject({ refused: 1, built: 1 });
      // A refusal is not a build failure: the gate's own warn log reports it.
      expect(
         captured.logs.filter(
            (l) => l.message === "materialization.build.failed",
         ),
      ).toHaveLength(0);
   });

   it("fails a refusal thrown from inside the build as deterministic, with its reason as the code", async () => {
      const build = sinon.stub().callsFake(async (_s, i: BuildInstruction) => {
         if (i.sourceEntityId === "a1aaaaaaaaaaaaaa") {
            throw new MaterializationEligibilityError({
               message: "not eligible",
               reason: "free_parameter",
            });
         }
         return entryFor(i.sourceEntityId);
      });
      await runLoop(build);
      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            outcome: "failed",
            failure_class: "deterministic",
         }),
      ).toBe(1);
      const [failed] = captured.logs.filter(
         (l) => l.message === "materialization.build.failed",
      );
      expect(failed.meta.code).toBe("free_parameter");
   });
});

describe("run events", () => {
   let captured: ReturnType<typeof captureLogs>;
   beforeEach(() => {
      captured = captureLogs();
   });
   afterEach(() => captured.restore());

   it("logs run.started and run.finished with the spec's fields and levels", () => {
      const run = new RunTelemetry("run-9", "orchestrated", "pkg");
      run.started("orchestrator", 3);
      run.tally.built = 2;
      run.tally.failed = 1;
      run.finished("partial", 1200);
      run.finished("success", 10);
      run.finished(
         "failed",
         20,
         Object.assign(new Error("SELECT 'x' failed"), { code: "XX000" }),
      );
      run.finished("cancelled", 30, new Error("Build cancelled"));

      expect(captured.logs.map((l) => [l.message, l.level])).toEqual([
         ["materialization.run.started", "info"],
         ["materialization.run.finished", "warn"],
         ["materialization.run.finished", "info"],
         ["materialization.run.finished", "error"],
         ["materialization.run.finished", "info"],
      ]);
      expect(captured.logs[0].meta).toEqual({
         runId: "run-9",
         packageName: "pkg",
         mode: "orchestrated",
         trigger: "orchestrator",
         sourceCount: 3,
      });
      expect(captured.logs[1].meta).toEqual({
         runId: "run-9",
         packageName: "pkg",
         mode: "orchestrated",
         outcome: "partial",
         durationMs: 1200,
         built: 2,
         reused: 0,
         refused: 0,
         failed: 1,
      });
      expect(captured.logs[3].meta).toMatchObject({ code: "XX000" });
      expect(JSON.stringify(captured.logs)).not.toContain("SELECT");
      // A cancellation keeps the code of what ended it: an abort can race a
      // genuine failure.
      expect(captured.logs[4].meta).toMatchObject({
         outcome: "cancelled",
         code: "Error",
      });
   });
});

describe("run telemetry review findings", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;
   let captured: ReturnType<typeof captureLogs>;

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      captured = captureLogs();
   });

   afterEach(async () => {
      captured.restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   it("counts a failed address once when a base and its extend share it", async () => {
      const connection = {
         runSQL: sinon.stub().resolves(),
      } as unknown as MalloyConnection;
      // A base and its `extend` (`#@ persist` is inherited) share one
      // content address: one table, one build.
      const compiled = compiledWith(
         {
            base: fakeSource({
               name: "base",
               sourceEntityId: "a1aaaaaaaaaaaaaa",
            }),
            ext: fakeSource({
               name: "ext",
               sourceEntityId: "a1aaaaaaaaaaaaaa",
            }),
            other: fakeSource({
               name: "other",
               sourceEntityId: "a2bbbbbbbbbbbbbb",
            }),
         },
         [["base", "ext"], ["other"]],
         new Map([["duckdb", connection]]),
      );
      const build = sinon.stub().callsFake(async (_s, i: BuildInstruction) => {
         if (i.sourceEntityId === "a1aaaaaaaaaaaaaa") {
            throw new Error("Permission denied");
         }
         return {
            sourceEntityId: i.sourceEntityId,
            physicalTableName: `${i.sourceEntityId}_t`,
            connectionName: "duckdb",
            buildDurationMs: 10,
         };
      });
      (ctx.service as unknown as { buildOneSource: unknown }).buildOneSource =
         build;
      const telemetry = new RunTelemetry("run-1", "auto", "pkg");
      const result = (await withRunTelemetry(telemetry, () =>
         (
            ctx.service as unknown as {
               executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
            }
         ).executeInstructedBuild(
            compiled,
            {
               getApiConnection: () => {
                  throw new Error("no config");
               },
               getEnvironmentPath: () => "/tmp/env",
            },
            [
               {
                  sourceEntityId: "a1aaaaaaaaaaaaaa",
                  materializedTableId: "mt-1",
                  physicalTableName: "t1",
                  realization: "COPY",
               },
               {
                  sourceEntityId: "a2bbbbbbbbbbbbbb",
                  materializedTableId: "mt-2",
                  physicalTableName: "t2",
                  realization: "COPY",
               },
            ],
            {},
            new AbortController().signal,
            false,
         ),
      )) as {
         failures: Record<
            string,
            { sourceName: string; reason: string; retryable?: boolean }
         >;
      };

      // The shared table's build ran once: the extend took the base's
      // failure rather than running the same build again.
      expect(
         build
            .getCalls()
            .filter(
               (c) =>
                  (c.args[1] as BuildInstruction).sourceEntityId ===
                  "a1aaaaaaaaaaaaaa",
            ),
      ).toHaveLength(1);
      // The run's record holds one failure for the address, named for the
      // last source on it as before, with the build's own reason ...
      expect(Object.keys(result.failures)).toEqual(["a1aaaaaaaaaaaaaa"]);
      expect(result.failures["a1aaaaaaaaaaaaaa"]).toMatchObject({
         sourceName: "ext",
         reason: "Permission denied",
         retryable: false,
      });
      // ... and the counter, the tally and the log agree with it.
      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            outcome: "failed",
         }),
      ).toBe(1);
      expect(telemetry.tally).toMatchObject({ built: 1, failed: 1 });
      expect(
         captured.logs.filter(
            (l) => l.message === "materialization.build.failed",
         ),
      ).toHaveLength(1);
   });

   it("reduces a wrapped driver error to the driver's code", () => {
      // The storage strategy rewraps every failure in a plain Error (its
      // message redacted) with the driver's error as the cause.
      const driver = Object.assign(new Error("SELECT 'v' FROM secret"), {
         code: "42P01",
      });
      expect(
         errorCodeOf(new Error("Failed to materialize", { cause: driver })),
      ).toBe("42P01");
      // A billed read lost keeps its own class name over a plain cause.
      expect(
         errorCodeOf(
            new BilledReadNotCapturedError("lost", {
               cause: new Error("x"),
            }),
         ),
      ).toBe("BilledReadNotCapturedError");
   });
});
