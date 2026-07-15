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
 * `materialization.schedule` cron so an open-source publisher (no control plane)
 * can rebuild on a cadence. Modeled on {@link PackageMemoryGovernor} — a
 * config-driven `setInterval` with `start`/`stop` and one `tick`.
 *
 * **Orchestration safety (two independent guards):**
 *   1. Disabled by default — only constructed when
 *      {@link getMaterializationSchedulerConfig} returns non-null
 *      (`PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER`), which an orchestrated
 *      deployment never sets, so the scheduler never runs there.
 *   2. Even when enabled, a package with a configured `manifestLocation` (i.e.
 *      control-plane-driven) is skipped — the CP already fires it, so we never
 *      double-drive.
 *
 * These guards are **independent and both load-bearing — never collapse them to
 * just the `manifestLocation` check.** A control-plane-loaded package that is
 * *serving live* has `manifestLocation === null`, so Guard 2 does not cover it;
 * only Guard 1 (the opt-in flag, which an orchestrated worker must never set —
 * see {@link getMaterializationSchedulerConfig}) keeps the scheduler off there.
 *
 * It only sweeps **already-loaded** packages (never forces a load), fires the
 * existing `createMaterialization` auto-run path tagged `trigger=SCHEDULER`, and
 * only fires packages whose persistence policy is valid (`scope: version`, no
 * `freshness` — the same rule a publish enforces). Arming state is in memory:
 * on restart it re-arms from `cron.nextAfter(now)`, so a fire that came due
 * during downtime is skipped (never double-fired). A freshly-armed package
 * never fires on the arming tick (`nextAfter` is strictly future), so a restart
 * does not trigger a rebuild.
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

               const armed = this.arm(key, schedule, nowMs);
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
    * changed. A fresh arm computes `nextAfter(now)` (strictly future), so a
    * newly-seen or restarted package never fires on the arming tick.
    */
   private arm(key: string, schedule: string, nowMs: number): ScheduleState {
      const existing = this.state.get(key);
      if (existing && existing.schedule === schedule) {
         return existing;
      }
      const armed: ScheduleState = {
         schedule,
         nextFireAtMs: this.cron.nextAfter(schedule, new Date(nowMs)).getTime(),
         lastFiredAtMs: existing?.lastFiredAtMs ?? null,
      };
      this.state.set(key, armed);
      return armed;
   }

   /**
    * Fire a whole-package scheduled rebuild via the existing auto-run path. An
    * already-active materialization coalesces (the in-flight build covers this
    * occurrence); any other error is logged and metered but never propagated.
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
