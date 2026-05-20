import { describe, expect, it } from "bun:test";

import type { MemoryGovernorConfig } from "../config";
import { PackageMemoryGovernor } from "./package_memory_governor";

const ONE_GB = 1024 * 1024 * 1024;

function makeConfig(
   overrides: Partial<MemoryGovernorConfig> = {},
): MemoryGovernorConfig {
   return {
      maxMemoryBytes: ONE_GB,
      highWaterFraction: 0.8,
      lowWaterFraction: 0.7,
      checkIntervalMs: 5_000,
      backpressureEnabled: true,
      ...overrides,
   };
}

/**
 * Test driver that lets us push a sequence of RSS values into the
 * governor and inspect the state machine's reactions deterministically
 * — no real allocations, no real timers.
 */
class FakeRssSampler {
   private value = 0;
   constructor(initial = 0) {
      this.value = initial;
   }
   set(value: number): void {
      this.value = value;
   }
   sampler = (): number => this.value;
}

describe("PackageMemoryGovernor", () => {
   it("does not activate back-pressure below the high-water mark", () => {
      const rss = new FakeRssSampler(0.5 * ONE_GB);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);
   });

   it("activates back-pressure at or above the high-water mark", () => {
      const rss = new FakeRssSampler(0);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);

      rss.set(0.79 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);

      // 0.8 * 1GB is exactly the high-water threshold; using >= so it
      // trips on the boundary.
      rss.set(0.8 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);
   });

   it("does not clear back-pressure inside the hysteresis band", () => {
      const rss = new FakeRssSampler(0.9 * ONE_GB);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);

      // Between low (0.7) and high (0.8) — must stay backpressured.
      rss.set(0.75 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);
   });

   it("clears back-pressure at or below the low-water mark", () => {
      const rss = new FakeRssSampler(0.9 * ONE_GB);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);

      // The implementation floors lowWaterBytes (= 0.7 * 1GB → 751619276),
      // so we need to feed a value at or below that integer — `0.7 * 1GB`
      // as a float is 751619276.8 which sits just above the threshold.
      rss.set(0.69 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);
   });

   it("re-activates after recovery if RSS climbs again", () => {
      const rss = new FakeRssSampler(0);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);

      rss.set(0.85 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);

      rss.set(0.6 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);

      rss.set(0.9 * ONE_GB);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);
   });

   it("samples but never flips the flag when backpressureEnabled=false", () => {
      const rss = new FakeRssSampler(0.95 * ONE_GB);
      const gov = new PackageMemoryGovernor(
         makeConfig({ backpressureEnabled: false }),
         rss.sampler,
      );
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);
      // Status still tracks RSS even though the flag is suppressed.
      expect(gov.getStatus().rssBytes).toBe(0.95 * ONE_GB);
   });

   it("survives a throwing sampler without crashing or flipping state", () => {
      let throwOnce = true;
      const gov = new PackageMemoryGovernor(makeConfig(), () => {
         if (throwOnce) {
            throwOnce = false;
            throw new Error("simulated sampling failure");
         }
         return 0.4 * ONE_GB;
      });

      // First tick: sampler throws; governor swallows it and leaves
      // the state untouched.
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);

      // Second tick succeeds.
      gov.tick();
      expect(gov.isBackpressured()).toBe(false);
   });

   it("start() takes an immediate sample so a hot-start respects the cap", () => {
      const rss = new FakeRssSampler(0.95 * ONE_GB);
      const gov = new PackageMemoryGovernor(
         // Big interval so we know the initial sample isn't from a
         // delayed tick.
         makeConfig({ checkIntervalMs: 60_000 }),
         rss.sampler,
      );
      gov.start();
      expect(gov.isBackpressured()).toBe(true);
      gov.stop();
   });

   it("stop() clears back-pressure and is idempotent", () => {
      const rss = new FakeRssSampler(0.95 * ONE_GB);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);
      gov.tick();
      expect(gov.isBackpressured()).toBe(true);

      gov.stop();
      expect(gov.isBackpressured()).toBe(false);
      // Second call is a no-op (no thrown error, flag stays cleared).
      gov.stop();
      expect(gov.isBackpressured()).toBe(false);
   });

   it("exposes computed threshold bytes through getStatus", () => {
      const rss = new FakeRssSampler(0.4 * ONE_GB);
      const gov = new PackageMemoryGovernor(makeConfig(), rss.sampler);
      gov.tick();
      const status = gov.getStatus();
      expect(status.maxMemoryBytes).toBe(ONE_GB);
      expect(status.highWaterBytes).toBe(Math.floor(0.8 * ONE_GB));
      expect(status.lowWaterBytes).toBe(Math.floor(0.7 * ONE_GB));
      expect(status.rssBytes).toBe(0.4 * ONE_GB);
      expect(status.backpressured).toBe(false);
      expect(typeof status.lastSampledAt).toBe("number");
   });
});
