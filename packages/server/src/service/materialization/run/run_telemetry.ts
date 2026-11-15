// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AsyncLocalStorage } from "async_hooks";
import { getPersistStorageMode } from "../../../config";
import { causeChain } from "../../../driver_errors";
import { errorCodeOf } from "./error_code";
import { logger } from "../../../logger";
import {
   type BuildKind,
   type BuildMethod,
   type BuildTier,
   type FailureClass,
   type MaterializationMode,
   type MaterializationOutcome,
   recordBuildDuration,
   recordBuildOutcome,
   recordBuildReadBytes,
} from "../../../materialization_metrics";
import type { CompiledBuildPlan } from "../plan/build_plan";
import { PERSISTED_VIEW_COMPANION_SUFFIX } from "../plan/persisted_view";
import type {
   BuildInstruction,
   BuildPlan,
   ManifestEntry,
} from "../domain/types";
import type { BuildRetryable } from "./build_failure_class";
import { BilledReadNotCapturedError } from "./materialization_build_session";

/**
 * The run and build events of materialization (plan section 5, "Step 4
 * specification"): `materialization.run.started` and `.run.finished` around a
 * run, `materialization.build.finished` and `.build.failed` per source, and the
 * build counters and histograms beside them. Each log is one call with the
 * event name as its message, so a pipeline can key on it; `runId` (the run's
 * materialization id) and `sourceEntityId` (a digest) join it to the run's
 * record, which holds the rest.
 *
 * Never on an event or a label: row values, given values, gate text, SQL, a
 * driver's message (it can quote SQL; reduced to {@link errorCodeOf}),
 * storage aliases or destination locations, credentials. A host-assigned table
 * name is left off too: only a name Publisher made is logged.
 */

/** What started a run, on `materialization.run.started`. */
export const RUN_TRIGGERS = [
   "schedule",
   "api",
   "auto",
   "orchestrator",
] as const;
type RunTrigger = (typeof RUN_TRIGGERS)[number];

/**
 * A run's trigger: the scheduler's fire, else a host's instructed run, else a
 * run a publish started, else an explicit request.
 */
export function runTriggerOf(args: {
   trigger: "ON_DEMAND" | "SCHEDULER";
   runContextTrigger?: string;
   orchestrated: boolean;
}): RunTrigger {
   if (args.trigger === "SCHEDULER" || args.runContextTrigger === "scheduler") {
      return "schedule";
   }
   if (args.orchestrated) return "orchestrator";
   if (args.runContextTrigger === "publish") return "auto";
   return "api";
}

/** What one planned source's build makes (see {@link BuildKind}). */
export function buildKindOf(
   compiled: Pick<CompiledBuildPlan, "preaggregatePlans" | "materializePlans">,
   sourceID: string | undefined,
): BuildKind {
   if (sourceID === undefined) return "persist";
   if (compiled.preaggregatePlans?.[sourceID]) return "rollup";
   const materialize = compiled.materializePlans?.[sourceID];
   if (materialize) return materialize.pair ? "pair" : "materialize";
   if (sourceID.endsWith(PERSISTED_VIEW_COMPANION_SUFFIX)) return "view";
   return "persist";
}

/**
 * Each planned content address's kind, for a source the run carried forward
 * and so knows only by address. A base and its `extend` share an address and
 * a kind; an address the plan does not list is left to the caller.
 */
export function buildKindsByAddress(
   compiled: Pick<CompiledBuildPlan, "preaggregatePlans" | "materializePlans">,
   plan: Pick<BuildPlan, "sources"> | null | undefined,
): Map<string, BuildKind> {
   const kinds = new Map<string, BuildKind>();
   for (const [sourceID, source] of Object.entries(plan?.sources ?? {})) {
      if (!kinds.has(source.sourceEntityId)) {
         kinds.set(source.sourceEntityId, buildKindOf(compiled, sourceID));
      }
   }
   return kinds;
}

/** The tier an instruction builds into, decided as the build decides it. */
export function buildTierOf(
   instruction: Pick<BuildInstruction, "destination">,
): BuildTier {
   return instruction.destination && getPersistStorageMode() !== "off"
      ? "storage"
      : "colocated";
}

/**
 * A failed build's class: `billed_read_lost` when its warehouse read was
 * charged and not captured, else its `BuildRetryable` (false deterministic,
 * true transient, undefined unclassified).
 */
export function failureClassOf(
   err: unknown,
   retryable: BuildRetryable,
): Exclude<FailureClass, "none"> {
   for (const e of causeChain(err)) {
      if (e instanceof BilledReadNotCapturedError) return "billed_read_lost";
   }
   if (retryable === false) return "deterministic";
   if (retryable === true) return "transient";
   return "unclassified";
}

type SourceTally = Record<
   "built" | "reused" | "refused" | "failed" | "cancelled",
   number
>;

/** One run's identity and its per-source tally, for the run events. */
export class RunTelemetry {
   readonly tally: SourceTally = {
      built: 0,
      reused: 0,
      refused: 0,
      failed: 0,
      cancelled: 0,
   };
   /**
    * The addresses this run built, so a host's reference to one of them is
    * not counted again as reused.
    */
   readonly builtAddresses = new Set<string>();

   constructor(
      readonly runId: string,
      readonly mode: MaterializationMode,
      readonly packageName: string,
   ) {}

   /** `materialization.run.started`, once the run holds its tables. */
   started(trigger: RunTrigger, sourceCount: number): void {
      logger.info("materialization.run.started", {
         runId: this.runId,
         packageName: this.packageName,
         mode: this.mode,
         trigger,
         sourceCount,
      });
   }

   /**
    * `materialization.run.finished`: info, warn when partial, error when
    * failed. The counts are the sources the build counter counted for this
    * run; the error that ended it, if any, is reduced to its code.
    */
   finished(
      outcome: MaterializationOutcome,
      durationMs: number,
      err?: unknown,
   ): void {
      const { built, reused, refused, failed, cancelled } = this.tally;
      const level =
         outcome === "failed"
            ? "error"
            : outcome === "partial"
              ? "warn"
              : "info";
      logger[level]("materialization.run.finished", {
         runId: this.runId,
         packageName: this.packageName,
         mode: this.mode,
         outcome,
         durationMs,
         built,
         reused,
         refused,
         failed,
         ...(cancelled > 0 ? { cancelled } : {}),
         // A cancelled run's too: an abort can race a genuine failure.
         ...(err !== undefined ? { code: errorCodeOf(err) } : {}),
      });
   }
}

// Run failures `materialization.run.finished` already reported, so the
// background runner does not log the same failure again.
const reportedRunFailures = new WeakSet<object>();

/** Mark `err` as reported by `materialization.run.finished`. */
export function markRunFailureReported(err: unknown): void {
   if (typeof err === "object" && err !== null) reportedRunFailures.add(err);
}

/** Whether `materialization.run.finished` already reported `err`. */
export function runFailureReported(err: unknown): boolean {
   return (
      typeof err === "object" && err !== null && reportedRunFailures.has(err)
   );
}

const currentRun = new AsyncLocalStorage<RunTelemetry>();

/** Run `fn` with `run` as the run every source event in it belongs to. */
export function withRunTelemetry<T>(
   run: RunTelemetry,
   fn: () => Promise<T>,
): Promise<T> {
   return currentRun.run(run, fn);
}

/** The fields every build event carries. */
interface SourceIdentity {
   kind: BuildKind;
   tier: BuildTier;
   sourceEntityId: string;
   sourceName: string;
   /** Logged only when Publisher made the name (an auto-run). */
   physicalTableName?: string;
}

function sourceFields(source: SourceIdentity, run: RunTelemetry | undefined) {
   return {
      ...(run ? { runId: run.runId, packageName: run.packageName } : {}),
      sourceEntityId: source.sourceEntityId,
      sourceName: source.sourceName,
      kind: source.kind,
      tier: source.tier,
      ...(source.physicalTableName && run?.mode === "auto"
         ? { physicalTable: source.physicalTableName }
         : {}),
   };
}

/**
 * A source the run built: counted, timed when the build did work (a skipped
 * incremental refresh reports no duration), and logged as
 * `materialization.build.finished`.
 */
export function reportSourceBuilt(
   source: SourceIdentity,
   entry: Pick<
      ManifestEntry,
      "buildDurationMs" | "rowCount" | "queryCostBytes" | "refresh"
   >,
): void {
   const run = currentRun.getStore();
   if (run) {
      run.tally.built++;
      run.builtAddresses.add(source.sourceEntityId);
   }
   const method: BuildMethod = entry.refresh === "delta" ? "delta" : "full";
   recordBuildOutcome({
      kind: source.kind,
      tier: source.tier,
      outcome: "built",
   });
   const durationMs = entry.buildDurationMs ?? undefined;
   if (durationMs !== undefined) {
      recordBuildDuration(durationMs, {
         kind: source.kind,
         tier: source.tier,
         method,
      });
   }
   recordBuildReadBytes(entry.queryCostBytes, source.tier);
   logger.info("materialization.build.finished", {
      ...sourceFields(source, run),
      method,
      ...(durationMs !== undefined ? { durationMs } : {}),
      ...(typeof entry.rowCount === "number" ? { rows: entry.rowCount } : {}),
      ...(typeof entry.queryCostBytes === "number" && entry.queryCostBytes > 0
         ? { bytesRead: entry.queryCostBytes }
         : {}),
      ...(entry.refresh === "none" ? { refresh: "skipped" } : {}),
   });
}

/**
 * A source whose build failed, or was cut short by the run's cancellation:
 * counted (a failure with its class), timed when its build had started, and
 * logged as `materialization.build.failed` at warn, or error when the failure
 * is unclassified (nothing recognized it, so it may be Publisher's own fault).
 * A cancellation is logged at info: it was asked for.
 */
export function reportSourceFailed(
   source: SourceIdentity & {
      method: BuildMethod;
      /** Absent when the source failed before its build started. */
      durationMs?: number;
      /** The configured destination name, for a `storage=` build. */
      destination?: string;
   },
   err: unknown,
   retryable: BuildRetryable,
   cancelled: boolean,
): void {
   const run = currentRun.getStore();
   const failureClass: FailureClass = cancelled
      ? "none"
      : failureClassOf(err, retryable);
   if (failureClass === "none") {
      if (run) run.tally.cancelled++;
      recordBuildOutcome({
         kind: source.kind,
         tier: source.tier,
         outcome: "cancelled",
      });
   } else {
      if (run) run.tally.failed++;
      recordBuildOutcome({
         kind: source.kind,
         tier: source.tier,
         outcome: "failed",
         failureClass,
      });
   }
   if (source.durationMs !== undefined) {
      recordBuildDuration(source.durationMs, {
         kind: source.kind,
         tier: source.tier,
         method: source.method,
      });
   }
   const level =
      failureClass === "none"
         ? "info"
         : failureClass === "unclassified"
           ? "error"
           : "warn";
   logger[level]("materialization.build.failed", {
      ...sourceFields(source, run),
      method: source.method,
      ...(source.durationMs !== undefined
         ? { durationMs: source.durationMs }
         : {}),
      outcome: failureClass === "none" ? "cancelled" : "failed",
      failureClass,
      code: errorCodeOf(err),
      ...(source.destination ? { destination: source.destination } : {}),
   });
}

/**
 * A source the eligibility gate refused: counted only. Its warn log, with the
 * refusal, is the gate's own.
 */
export function reportSourceRefused(kind: BuildKind, tier: BuildTier): void {
   const run = currentRun.getStore();
   if (run) run.tally.refused++;
   recordBuildOutcome({ kind, tier, outcome: "refused" });
}

/** A source the run carried forward instead of building: counted only. */
export function reportSourceReused(kind: BuildKind, tier: BuildTier): void {
   const run = currentRun.getStore();
   if (run) run.tally.reused++;
   recordBuildOutcome({ kind, tier, outcome: "reused" });
}
