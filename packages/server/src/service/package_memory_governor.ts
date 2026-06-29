import { publisherMeter } from "../telemetry";

import type { MemoryGovernorConfig } from "../config";
import { logger } from "../logger";

/**
 * Snapshot returned by {@link PackageMemoryGovernor.getStatus} for
 * health endpoints, tests, and ad-hoc logging.
 */
export interface MemoryGovernorStatus {
   rssBytes: number;
   maxMemoryBytes: number;
   highWaterBytes: number;
   lowWaterBytes: number;
   backpressured: boolean;
   /** Wall-clock ms of the last successful RSS sample. */
   lastSampledAt: number | null;
}

/**
 * Function that returns the current process RSS in bytes. Injectable
 * so unit tests can drive the governor with a deterministic source
 * without spinning real allocations.
 */
export type RssSampler = () => number;

const DEFAULT_RSS_SAMPLER: RssSampler = () => process.memoryUsage().rss;

/**
 * Polls process RSS on a fixed interval and toggles a single
 * `backpressured` flag using a low/high-water hysteresis band:
 *
 *  - RSS >= highWater → set `backpressured = true`
 *  - RSS <= lowWater  → set `backpressured = false`
 *  - in between       → leave the flag unchanged
 *
 * Controllers consult {@link isBackpressured} on hot paths that would
 * load a *new* package into memory (`addPackage`, reload, install) and
 * throw `ServiceUnavailableError` so the request fails fast as 503
 * instead of pushing the pod into an OOM kill.
 *
 * Already-loaded packages remain fully serviceable while back-pressure
 * is active — this is admission control on new memory, not a cache
 * eviction. Recovery happens naturally as in-flight traffic completes
 * and the kernel reclaims pages.
 *
 * Disabled by default; only constructed when
 * `getMemoryGovernorConfig()` returns a non-null config (driven by
 * `PUBLISHER_MAX_MEMORY_BYTES`).
 */
export class PackageMemoryGovernor {
   private readonly config: MemoryGovernorConfig;
   private readonly rssSampler: RssSampler;
   private readonly highWaterBytes: number;
   private readonly lowWaterBytes: number;
   private timer: ReturnType<typeof setInterval> | null = null;
   private backpressured = false;
   private lastSampledRss = 0;
   private lastSampledAt: number | null = null;
   private readonly backpressureActivationsCounter: ReturnType<
      ReturnType<typeof publisherMeter>["createCounter"]
   >;

   constructor(config: MemoryGovernorConfig, rssSampler?: RssSampler) {
      this.config = config;
      this.rssSampler = rssSampler ?? DEFAULT_RSS_SAMPLER;
      this.highWaterBytes = Math.floor(
         config.maxMemoryBytes * config.highWaterFraction,
      );
      this.lowWaterBytes = Math.floor(
         config.maxMemoryBytes * config.lowWaterFraction,
      );

      const meter = publisherMeter();

      // Periodic gauge: current process RSS in bytes.
      meter
         .createObservableGauge("publisher_process_rss_bytes", {
            description:
               "Current resident set size of the publisher process in bytes",
            unit: "By",
         })
         .addCallback((observation) => {
            observation.observe(this.rssSampler());
         });

      // Periodic gauge: 1 when admission control is rejecting new
      // package loads, 0 otherwise.
      meter
         .createObservableGauge("publisher_memory_backpressure_active", {
            description:
               "1 when the publisher is rejecting new package loads to stay under PUBLISHER_MAX_MEMORY_BYTES; 0 otherwise",
         })
         .addCallback((observation) => {
            observation.observe(this.backpressured ? 1 : 0);
         });

      // Cumulative counter for how many times we have transitioned
      // from `false → true`. Useful for alerting on a flapping pod.
      this.backpressureActivationsCounter = meter.createCounter(
         "publisher_memory_backpressure_activations_total",
         {
            description:
               "Number of times the memory governor has activated back-pressure",
         },
      );

      // Static gauges so dashboards can render the band alongside RSS
      // without needing to plumb config separately.
      meter
         .createObservableGauge("publisher_memory_max_bytes", {
            description: "Configured PUBLISHER_MAX_MEMORY_BYTES",
            unit: "By",
         })
         .addCallback((observation) =>
            observation.observe(this.config.maxMemoryBytes),
         );
      meter
         .createObservableGauge("publisher_memory_high_water_bytes", {
            description: "RSS threshold at which back-pressure activates",
            unit: "By",
         })
         .addCallback((observation) =>
            observation.observe(this.highWaterBytes),
         );
      meter
         .createObservableGauge("publisher_memory_low_water_bytes", {
            description: "RSS threshold at which back-pressure clears",
            unit: "By",
         })
         .addCallback((observation) => observation.observe(this.lowWaterBytes));
   }

   /**
    * Begin periodic RSS sampling. Safe to call multiple times — extra
    * calls are no-ops. The interval is `.unref()`'d so the governor
    * does not keep the process alive on its own.
    */
   public start(): void {
      if (this.timer !== null) return;
      // Take an immediate sample so a freshly-started server with
      // pre-existing high RSS goes into back-pressure right away
      // instead of waiting `checkIntervalMs` for the first tick.
      this.tick();
      this.timer = setInterval(() => this.tick(), this.config.checkIntervalMs);
      // Tolerate environments without Timer#unref (e.g. some bundlers).
      (
         this.timer as ReturnType<typeof setInterval> & {
            unref?: () => void;
         }
      ).unref?.();
      logger.info(
         `PackageMemoryGovernor started (max=${this.config.maxMemoryBytes}B, high=${this.highWaterBytes}B, low=${this.lowWaterBytes}B, interval=${this.config.checkIntervalMs}ms, backpressure=${this.config.backpressureEnabled})`,
      );
   }

   /**
    * Stop the periodic sampler. Idempotent. Clears the back-pressure
    * flag so any in-process logic that consults
    * {@link isBackpressured} during shutdown sees a permissive state.
    */
   public stop(): void {
      if (this.timer !== null) {
         clearInterval(this.timer);
         this.timer = null;
      }
      this.backpressured = false;
   }

   /**
    * Sample RSS once and apply the hysteresis band. Exposed (rather
    * than kept private) so callers can force a fresh check right
    * after they finish loading a new package, and so tests can drive
    * the governor synchronously.
    */
   public tick(): void {
      let rss: number;
      try {
         rss = this.rssSampler();
      } catch (err) {
         // Sampling failures must never crash the server. Log and
         // skip; the next interval will retry. Leave the flag
         // unchanged so we neither over- nor under-react to a single
         // measurement glitch.
         logger.error("PackageMemoryGovernor: RSS sample failed", {
            error: err,
         });
         return;
      }
      this.lastSampledRss = rss;
      this.lastSampledAt = Date.now();

      if (!this.config.backpressureEnabled) {
         // Feature dial: keep sampling for metrics but never flip
         // the flag. Useful for monitoring-only rollouts before
         // enabling the actual 503 behaviour.
         return;
      }

      if (rss >= this.highWaterBytes && !this.backpressured) {
         this.backpressured = true;
         this.backpressureActivationsCounter.add(1);
         logger.warn(
            `PackageMemoryGovernor: activating back-pressure (rss=${rss}B >= high=${this.highWaterBytes}B). New package loads will be rejected with HTTP 503 until rss <= ${this.lowWaterBytes}B.`,
         );
      } else if (rss <= this.lowWaterBytes && this.backpressured) {
         this.backpressured = false;
         logger.info(
            `PackageMemoryGovernor: clearing back-pressure (rss=${rss}B <= low=${this.lowWaterBytes}B).`,
         );
      }
   }

   /**
    * True iff new package-load requests should be rejected with HTTP
    * 503. Cheap O(1) read of a private boolean; safe to call on every
    * request.
    */
   public isBackpressured(): boolean {
      return this.backpressured;
   }

   public getStatus(): MemoryGovernorStatus {
      return {
         rssBytes: this.lastSampledRss,
         maxMemoryBytes: this.config.maxMemoryBytes,
         highWaterBytes: this.highWaterBytes,
         lowWaterBytes: this.lowWaterBytes,
         backpressured: this.backpressured,
         lastSampledAt: this.lastSampledAt,
      };
   }
}
