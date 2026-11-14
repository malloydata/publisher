// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { lazyCounter, lazyInstrument, publisherMeter } from "../telemetry";

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

/** Counts false → true transitions, for alerting on a flapping pod. */
const backpressureActivations = lazyCounter(
   "publisher_memory_backpressure_activations_total",
   "Number of times the memory governor has activated back-pressure",
);

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
   /** The governor the gauges read: the one constructed last. */
   private static observed: PackageMemoryGovernor | null = null;
   private static gauges = lazyInstrument(() => {
      const meter = publisherMeter();
      const governor = () => PackageMemoryGovernor.observed;
      const gauge = (
         name: string,
         description: string,
         read: (g: PackageMemoryGovernor) => number,
         unit?: string,
      ) => {
         const instrument = meter.createObservableGauge(name, {
            description,
            ...(unit ? { unit } : {}),
         });
         instrument.addCallback((observation) => {
            const g = governor();
            if (g) observation.observe(read(g));
         });
         return instrument;
      };
      return [
         gauge(
            "publisher_process_rss_bytes",
            "Current resident set size of the publisher process in bytes",
            (g) => g.rssSampler(),
            "By",
         ),
         gauge(
            "publisher_memory_backpressure_active",
            "1 when the publisher is rejecting new package loads to stay under PUBLISHER_MAX_MEMORY_BYTES; 0 otherwise",
            (g) => (g.backpressured ? 1 : 0),
         ),
         // The band itself, so dashboards can draw it alongside RSS.
         gauge(
            "publisher_memory_max_bytes",
            "Configured PUBLISHER_MAX_MEMORY_BYTES",
            (g) => g.config.maxMemoryBytes,
            "By",
         ),
         gauge(
            "publisher_memory_high_water_bytes",
            "RSS threshold at which back-pressure activates",
            (g) => g.highWaterBytes,
            "By",
         ),
         gauge(
            "publisher_memory_low_water_bytes",
            "RSS threshold at which back-pressure clears",
            (g) => g.lowWaterBytes,
            "By",
         ),
      ];
   });

   constructor(config: MemoryGovernorConfig, rssSampler?: RssSampler) {
      this.config = config;
      this.rssSampler = rssSampler ?? DEFAULT_RSS_SAMPLER;
      this.highWaterBytes = Math.floor(
         config.maxMemoryBytes * config.highWaterFraction,
      );
      this.lowWaterBytes = Math.floor(
         config.maxMemoryBytes * config.lowWaterFraction,
      );

      PackageMemoryGovernor.observed = this;
      PackageMemoryGovernor.gauges();
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
         backpressureActivations().add(1);
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
