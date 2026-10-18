// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { MaterializationSchedulerConfig } from "../config";
import { MaterializationConflictError } from "../errors";
import { logger } from "../logger";
import { recordScheduledFire } from "../materialization_metrics";
import { CronEvaluator } from "./cron_evaluator";
import type { EnvironmentStore } from "./environment_store";
import type { MaterializationService } from "./materialization_service";

/** Per-package arming state, keyed by `${environmentName}::${packageName}`. */
interface ScheduleState {
   /** The cron the state was armed from; a change re-arms `nextFireAtMs`. */
   schedule: string;
   /** Epoch ms of the next due fire (`cron.nextAfter`, strictly future when armed). */
   nextFireAtMs: number;
   /** Epoch ms of the last fire attempt, or null if never fired. */
   lastFiredAtMs: number | null;
}

/**
 * Standalone materialization scheduler: fires a loaded package's
 * `materialization.schedule` cron so a publisher with no control plane can
 * rebuild on a cadence.
 *
 * **Orchestration safety (two independent guards):**
 *   1. Disabled by default: only constructed when
 *      {@link getMaterializationSchedulerConfig} returns non-null
 *      (`PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER`), which an orchestrated
 *      deployment never sets.
 *   2. A package with a `manifestLocation` (control-plane-driven) is skipped;
 *      the control plane already fires it.
 *
 * **Both guards are load-bearing; never collapse them to the `manifestLocation`
 * check.** A control-plane-loaded package that is *serving live* has
 * `manifestLocation === null`, so only Guard 1 keeps the scheduler off there.
 *
 * Sweeps only **already-loaded** packages (never forces a load) and only those
 * whose persistence policy is valid (the publish rule). Arming state is in
 * memory, re-anchored after a restart from the newest recorded `SCHEDULER` fire
 * (see {@link arm}).
 */
export class MaterializationScheduler {
   private timer: ReturnType<typeof setInterval> | null = null;
   private readonly cron: CronEvaluator;
   private readonly state = new Map<string, ScheduleState>();

   constructor(
      private readonly environmentStore: EnvironmentStore,
      private readonly materializationService: MaterializationService,
      private readonly config: MaterializationSchedulerConfig,
      cron?: CronEvaluator,
   ) {
      this.cron = cron ?? new CronEvaluator();
   }

   /** Begin the periodic sweep. Idempotent; the interval is `unref`'d. */
   public start(): void {
      if (this.timer !== null) return;
      this.timer = setInterval(() => {
         void this.tick();
      }, this.config.tickIntervalMs);
      (
         this.timer as ReturnType<typeof setInterval> & { unref?: () => void }
      ).unref?.();
      logger.info(
         `MaterializationScheduler started (interval=${this.config.tickIntervalMs}ms, maxFiresPerTick=${this.config.maxFiresPerTick})`,
      );
   }

   /** Stop the periodic sweep. Idempotent. */
   public stop(): void {
      if (this.timer !== null) {
         clearInterval(this.timer);
         this.timer = null;
      }
   }

   /**
    * One sweep: arm newly-seen schedules, fire due ones (capped per tick), and
    * prune state for packages no longer loaded/scheduled. Exposed so tests can
    * drive it synchronously. Never throws — per-package errors are isolated.
    */
   public async tick(nowMs: number = Date.now()): Promise<void> {
      const seen = new Set<string>();
      let fired = 0;

      for (const env of this.environmentStore.getLoadedEnvironments()) {
         const environmentName = env.getEnvironmentName();
         for (const pkg of env.getLoadedPackages()) {
            const packageName = pkg.getPackageName();
            const key = `${environmentName}::${packageName}`;
            try {
               const schedule = this.schedulableCron(pkg);
               if (schedule === null) {
                  // Not a standalone-schedulable package (no cron, orchestrated,
                  // or invalid policy/cron) — drop any stale arming state.
                  this.state.delete(key);
                  continue;
               }
               seen.add(key);

               const armed = await this.arm(
                  environmentName,
                  packageName,
                  key,
                  schedule,
                  nowMs,
               );
               if (nowMs < armed.nextFireAtMs) continue; // not due yet

               if (fired >= this.config.maxFiresPerTick) {
                  // Stampede guard: leave it due; it fires on a later tick.
                  continue;
               }
               await this.fire(environmentName, packageName);
               fired++;
               // Advance regardless of outcome so a persistent failure retries on
               // the next cron occurrence (cron cadence), never every tick.
               armed.nextFireAtMs = this.cron
                  .nextAfter(schedule, new Date(nowMs))
                  .getTime();
               armed.lastFiredAtMs = nowMs;
            } catch (err) {
               logger.warn("MaterializationScheduler: package sweep failed", {
                  environmentName,
                  packageName,
                  error: err instanceof Error ? err.message : String(err),
               });
            }
         }
      }

      for (const key of [...this.state.keys()]) {
         if (!seen.has(key)) this.state.delete(key);
      }
   }

   /**
    * The package's schedulable cron, or null when it must be skipped: no
    * `materialization.schedule`; a `manifestLocation` (control-plane-driven —
    * Guard 2); an invalid persistence policy (e.g. schedule without
    * `scope: version`, or schedule + freshness — the publish-gate rules); or an
    * unparseable cron. Skips are debug-logged (the policy issues are already
    * warned at package load), not warned each tick.
    */
   private schedulableCron(pkg: {
      getPackageMetadata(): {
         materialization?: { schedule?: string | null } | null;
         manifestLocation?: string | null;
      };
      persistencePolicyWarnings(): string[];
      getPackageName(): string;
   }): string | null {
      const metadata = pkg.getPackageMetadata();
      const schedule = metadata.materialization?.schedule ?? null;
      if (!schedule) return null;
      if (metadata.manifestLocation) return null; // orchestrated — CP fires it
      if (pkg.persistencePolicyWarnings().length > 0) {
         logger.debug(
            "MaterializationScheduler: skipping package with invalid persistence policy",
            { packageName: pkg.getPackageName() },
         );
         return null;
      }
      if (!this.cron.isValid(schedule)) {
         logger.debug("MaterializationScheduler: skipping invalid cron", {
            packageName: pkg.getPackageName(),
            schedule,
         });
         return null;
      }
      return schedule;
   }

   /**
    * Return the package's arming state, (re)arming when it is new or its cron
    * changed.
    *
    * **First arm this process**: anchor to `nextAfter(lastScheduledFire)`. If
    * that is already past (it came due during downtime), it fires once on this
    * tick and jumps forward: exactly one catch-up, matching the control plane's
    * persisted-`next_fire_at` recovery, never a burst. With no recorded fire it
    * falls back to `nextAfter(now)`, so a never-fired schedule does not fire on
    * the arming tick (and a schedule set while the scheduler was disabled is not
    * caught up on first enable).
    *
    * **Cron changed**: re-arm from `nextAfter(now)`, with no catch-up for the
    * old cron.
    */
   private async arm(
      environmentName: string,
      packageName: string,
      key: string,
      schedule: string,
      nowMs: number,
   ): Promise<ScheduleState> {
      const existing = this.state.get(key);
      if (existing && existing.schedule === schedule) {
         return existing;
      }

      const anchor =
         existing === undefined
            ? await this.recoveryAnchor(environmentName, packageName, nowMs)
            : new Date(nowMs);

      const armed: ScheduleState = {
         schedule,
         nextFireAtMs: this.cron.nextAfter(schedule, anchor).getTime(),
         lastFiredAtMs: existing?.lastFiredAtMs ?? null,
      };
      this.state.set(key, armed);
      return armed;
   }

   /**
    * The instant to compute the first `nextFireAt` from after a (re)start: the
    * newest recorded `SCHEDULER` fire if any, else `now`. Any lookup failure
    * degrades to `now` (no catch-up) so arming always succeeds.
    */
   private async recoveryAnchor(
      environmentName: string,
      packageName: string,
      nowMs: number,
   ): Promise<Date> {
      try {
         const last =
            await this.materializationService.getLatestScheduledFireAt(
               environmentName,
               packageName,
            );
         return last ?? new Date(nowMs);
      } catch (err) {
         logger.warn(
            "MaterializationScheduler: could not read last scheduled fire; arming from now",
            {
               environmentName,
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return new Date(nowMs);
      }
   }

   /**
    * Fire a whole-package rebuild via the `createMaterialization` auto-run path
    * (identical to an on-demand run but for the `trigger` label). An
    * already-active materialization coalesces (the in-flight build covers this
    * occurrence); any other error is logged and metered but never propagated.
    * Standalone fires build tables only; the indexed-dimension pass is a hosted
    * two-phase job concern.
    */
   private async fire(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      try {
         await this.materializationService.createMaterialization(
            environmentName,
            packageName,
            { forceRefresh: true, trigger: "SCHEDULER" },
         );
         recordScheduledFire("fired");
         logger.info("MaterializationScheduler: fired scheduled rebuild", {
            environmentName,
            packageName,
         });
      } catch (err) {
         if (err instanceof MaterializationConflictError) {
            recordScheduledFire("conflict");
            logger.debug(
               "MaterializationScheduler: skipped fire; a materialization is already active",
               { environmentName, packageName },
            );
            return;
         }
         recordScheduledFire("error");
         logger.warn("MaterializationScheduler: scheduled fire failed", {
            environmentName,
            packageName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }
}
