// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { publisherMeter } from "../telemetry";

import type { MemoryGovernorConfig } from "../config";
import { logger } from "../logger";

/** Snapshot returned by {@link PackageMemoryGovernor.getStatus}. */
export interface MemoryGovernorStatus {
   rssBytes: number;
   maxMemoryBytes: number;
   highWaterBytes: number;
   lowWaterBytes: number;
   backpressured: boolean;
   /** Wall-clock ms of the last successful RSS sample. */
   lastSampledAt: number | null;
}

/** Returns the current process RSS in bytes; injectable for tests. */
export type RssSampler = () => number;

const DEFAULT_RSS_SAMPLER: RssSampler = () => process.memoryUsage().rss;

/**
 * Polls process RSS and toggles `backpressured` with a hysteresis band: on at
 * highWater, off at lowWater, unchanged in between. Paths that would load a
 * NEW package (add, reload, install) check it and fail fast with a 503 rather
 * than push the pod into an OOM kill. It is admission control, not eviction:
 * loaded packages keep serving.
 *
 * Only constructed when `PUBLISHER_MAX_MEMORY_BYTES` is set.
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

      meter
         .createObservableGauge("publisher_process_rss_bytes", {
            description:
               "Current resident set size of the publisher process in bytes",
            unit: "By",
         })
         .addCallback((observation) => {
            observation.observe(this.rssSampler());
         });

      meter
         .createObservableGauge("publisher_memory_backpressure_active", {
            description:
               "1 when the publisher is rejecting new package loads to stay under PUBLISHER_MAX_MEMORY_BYTES; 0 otherwise",
         })
         .addCallback((observation) => {
            observation.observe(this.backpressured ? 1 : 0);
         });

      // Counts false → true transitions, for alerting on a flapping pod.
      this.backpressureActivationsCounter = meter.createCounter(
         "publisher_memory_backpressure_activations_total",
         {
            description:
               "Number of times the memory governor has activated back-pressure",
         },
      );

      // The band itself, so dashboards can draw it alongside RSS.
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
    * Begin periodic RSS sampling. Idempotent; the interval is unref'd so it
    * does not keep the process alive.
    */
   public start(): void {
      if (this.timer !== null) return;
      // Sample now so a server that starts with high RSS is back-pressured
      // before the first interval.
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

   /** Stop sampling (idempotent) and clear back-pressure for shutdown. */
   public stop(): void {
      if (this.timer !== null) {
         clearInterval(this.timer);
         this.timer = null;
      }
      this.backpressured = false;
   }

   /**
    * Sample RSS once and apply the band. Public so a caller can force a check
    * right after a load, and tests can drive it synchronously.
    */
   public tick(): void {
      let rss: number;
      try {
         rss = this.rssSampler();
      } catch (err) {
         // Never crash on a failed sample; leave the flag as it is and retry
         // on the next interval.
         logger.error("PackageMemoryGovernor: RSS sample failed", {
            error: err,
         });
         return;
      }
      this.lastSampledRss = rss;
      this.lastSampledAt = Date.now();

      if (!this.config.backpressureEnabled) {
         // Monitoring-only mode: sample for metrics, never flip the flag.
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

   /** True iff new package loads should be rejected with a 503. */
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
