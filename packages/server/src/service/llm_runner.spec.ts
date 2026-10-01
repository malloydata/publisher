// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   LlmError,
   type LlmProvider,
   type LlmRequest,
   type LlmResponse,
} from "./llm_provider";
import {
   LlmBudget,
   LlmRunner,
   type Clock,
   type RunnerSettings,
} from "./llm_runner";

const SETTINGS: RunnerSettings = {
   timeoutMs: 1000,
   maxAttempts: 3,
   backoffMs: 100,
   concurrency: 2,
   temperature: 0,
   seed: 7,
   jsonMode: "none",
   extraBody: {},
   breaker: { failures: 2, cooldownMs: 5000 },
   cacheMaxEntries: 2,
};

/** A clock the test moves by hand; sleep advances it instead of waiting. */
function fakeClock(): Clock & { t: number; slept: number[] } {
   const c = {
      t: 1_000,
      slept: [] as number[],
      now: () => c.t,
      sleep: async (ms: number) => {
         c.slept.push(ms);
         c.t += ms;
      },
   };
   return c;
}

const ok = (text: string): LlmResponse => ({
   text,
   model: "m",
   usage: { promptTokens: 10, completionTokens: 5 },
   latencyMs: 1,
});

const ARGS = { stage: "refine" as const, model: "m", user: "u" };

function scripted(
   script: Array<LlmResponse | LlmError>,
   seen: LlmRequest[] = [],
): LlmProvider {
   let i = 0;
   return {
      id: "fake",
      async complete(req) {
         seen.push(req);
         const step = script[Math.min(i++, script.length - 1)];
         if (step instanceof LlmError) throw step;
         return step;
      },
   };
}

const retryable = (kind: "timeout" | "http" = "timeout") =>
   new LlmError("boom", kind, true);

describe("LlmRunner retries", () => {
   it("returns the first success without retrying", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(scripted([ok("a")], seen), SETTINGS, fakeClock());
      const out = await r.complete(null, ARGS);
      expect(out.text).toBe("a");
      expect(out.cached).toBe(false);
      expect(seen).toHaveLength(1);
      expect(seen[0].temperature).toBe(0);
      expect(seen[0].seed).toBe(7);
   });

   it("retries a retryable failure with exponential backoff", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([retryable(), retryable(), ok("third")]),
         SETTINGS,
         clock,
      );
      expect((await r.complete(null, ARGS)).text).toBe("third");
      expect(clock.slept).toEqual([100, 200]);
   });

   it("gives up after maxAttempts and throws the last error", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(
         scripted([retryable("http")], seen),
         SETTINGS,
         fakeClock(),
      );
      const err = await r.complete(null, ARGS).catch((e) => e);
      expect(err).toBeInstanceOf(LlmError);
      expect(seen).toHaveLength(3);
   });

   it("does not retry a non-retryable error", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(
         scripted([new LlmError("no", "auth", false)], seen),
         SETTINGS,
         fakeClock(),
      );
      await r.complete(null, ARGS).catch(() => undefined);
      expect(seen).toHaveLength(1);
   });

   it("honors Retry-After over the computed backoff", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([
            new LlmError("slow", "rate_limit", true, 429, 2500),
            ok("a"),
         ]),
         SETTINGS,
         clock,
      );
      await r.complete(null, ARGS);
      expect(clock.slept).toEqual([2500]);
   });

   it("wraps a non-LlmError throw as a retryable network error", async () => {
      let n = 0;
      const provider: LlmProvider = {
         id: "fake",
         async complete() {
            if (n++ === 0) throw new Error("kaboom");
            return ok("b");
         },
      };
      const r = new LlmRunner(provider, SETTINGS, fakeClock());
      expect((await r.complete(null, ARGS)).text).toBe("b");
   });
});

describe("LlmRunner budget", () => {
   it("counts each attempt against the call budget", async () => {
      const seen: LlmRequest[] = [];
      const clock = fakeClock();
      const r = new LlmRunner(scripted([retryable()], seen), SETTINGS, clock);
      const budget = new LlmBudget(2, 60_000, clock.now);
      const err = await r.complete(budget, ARGS).catch((e) => e);
      expect(err).toBeInstanceOf(LlmError);
      expect(seen).toHaveLength(2);
      expect(budget.callsLeft()).toBe(0);
   });

   it("refuses to start once the call budget is spent", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(scripted([ok("a")]), SETTINGS, clock);
      const budget = new LlmBudget(1, 60_000, clock.now);
      await r.complete(budget, ARGS);
      const err = await r.complete(budget, ARGS).catch((e) => e);
      expect(err.kind).toBe("budget");
   });

   it("clamps the per-call timeout to the time left", async () => {
      const seen: LlmRequest[] = [];
      const clock = fakeClock();
      const r = new LlmRunner(scripted([ok("a")], seen), SETTINGS, clock);
      const budget = new LlmBudget(5, 300, clock.now);
      await r.complete(budget, ARGS);
      expect(seen[0].timeoutMs).toBe(300);
      const wide = new LlmBudget(5, 60_000, clock.now);
      await r.complete(wide, ARGS);
      expect(seen[1].timeoutMs).toBe(1000);
   });

   it("fails now rather than sleep past the deadline", async () => {
      const clock = fakeClock();
      // An HTTP error, not a timeout: a timeout with under a full call's worth of
      // budget left is reported as the budget running out and is not retried.
      const r = new LlmRunner(scripted([retryable("http")]), SETTINGS, clock);
      // 150ms left; the first backoff is 100ms then 200ms.
      const budget = new LlmBudget(10, 150, clock.now);
      const err = await r.complete(budget, ARGS).catch((e) => e);
      expect(err).toBeInstanceOf(LlmError);
      expect(clock.slept).toEqual([100]);
   });

   it("applies the deadline to calls that only get a slot after waiting for one", async () => {
      // Six jobs start at once and queue for one slot. Each call takes 100ms.
      // With 250ms of budget, the calls that start at 0, 100 and 200 run and the
      // rest find the deadline gone. Checked before the queue instead, all six
      // passed at t=0 and every one of them ran.
      const clock = fakeClock();
      const seen: LlmRequest[] = [];
      const provider: LlmProvider = {
         id: "slow",
         async complete(req) {
            seen.push(req);
            clock.t += 100;
            return ok("a");
         },
      };
      const r = new LlmRunner(provider, { ...SETTINGS, concurrency: 1 }, clock);
      const budget = new LlmBudget(50, 250, clock.now);
      const results = await Promise.allSettled(
         Array.from({ length: 6 }, () => r.complete(budget, ARGS)),
      );
      expect(seen).toHaveLength(3);
      const failed = results.filter(
         (x) => x.status === "rejected",
      ) as PromiseRejectedResult[];
      expect(failed).toHaveLength(3);
      expect(
         failed.every((f) => (f.reason as LlmError).kind === "budget"),
      ).toBe(true);
   });

   it("reports a timeout the budget caused as the budget, and does not count it against the endpoint", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([retryable("timeout")]),
         SETTINGS,
         clock,
      );
      // 50ms left, under the 1000ms per-call timeout: the call was cut short.
      for (let i = 0; i < 3; i++) {
         const err = await r
            .complete(new LlmBudget(5, 50, clock.now), ARGS)
            .catch((e) => e);
         expect((err as LlmError).kind).toBe("budget");
      }
      // Three such calls with a breaker at two failures, and it is still closed.
      expect(r.breakerOpen()).toBe(false);
      // A timeout at the full per-call limit is the endpoint's, and does count.
      for (let i = 0; i < 2; i++) {
         await r
            .complete(new LlmBudget(5, 60_000, clock.now), ARGS)
            .catch(() => {});
      }
      expect(r.breakerOpen()).toBe(true);
   });

   it("tallies calls, tokens and failures on the budget", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([retryable(), ok("a")]),
         SETTINGS,
         clock,
      );
      const budget = new LlmBudget(5, 60_000, clock.now);
      await r.complete(budget, ARGS);
      expect(budget.usage.calls).toBe(2);
      expect(budget.usage.failures).toBe(1);
      expect(budget.usage.promptTokens).toBe(10);
      expect(budget.usage.completionTokens).toBe(5);
   });
});

describe("LlmRunner circuit breaker", () => {
   it("opens after consecutive failures and skips the provider", async () => {
      const seen: LlmRequest[] = [];
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([new LlmError("down", "http", false, 500)], seen),
         { ...SETTINGS, maxAttempts: 1 },
         clock,
      );
      await r.complete(null, ARGS).catch(() => undefined);
      expect(r.breakerOpen()).toBe(false);
      await r.complete(null, ARGS).catch(() => undefined);
      expect(r.breakerOpen()).toBe(true);
      const err = await r.complete(null, ARGS).catch((e) => e);
      expect(err.kind).toBe("breaker");
      expect(seen).toHaveLength(2);
   });

   it("lets one call through after the cooldown and closes on success", async () => {
      const clock = fakeClock();
      let healthy = false;
      const provider: LlmProvider = {
         id: "fake",
         async complete() {
            if (!healthy) throw new LlmError("down", "http", false, 500);
            return ok("back");
         },
      };
      const r = new LlmRunner(provider, { ...SETTINGS, maxAttempts: 1 }, clock);
      await r.complete(null, ARGS).catch(() => undefined);
      await r.complete(null, ARGS).catch(() => undefined);
      expect(r.breakerOpen()).toBe(true);
      clock.t += 5001;
      expect(r.breakerOpen()).toBe(false);
      healthy = true;
      expect((await r.complete(null, ARGS)).text).toBe("back");
      await r.complete(null, ARGS);
      expect(r.breakerOpen()).toBe(false);
   });

   it("re-opens on a single failure after the cooldown", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(
         scripted([new LlmError("down", "http", false, 500)]),
         { ...SETTINGS, maxAttempts: 1 },
         clock,
      );
      await r.complete(null, ARGS).catch(() => undefined);
      await r.complete(null, ARGS).catch(() => undefined);
      clock.t += 5001;
      await r.complete(null, ARGS).catch(() => undefined);
      expect(r.breakerOpen()).toBe(true);
   });

   it("does not count a spent budget as an unhealthy endpoint", async () => {
      const clock = fakeClock();
      const r = new LlmRunner(scripted([ok("a")]), SETTINGS, clock);
      const spent = new LlmBudget(0, 60_000, clock.now);
      for (let i = 0; i < 5; i++) {
         await r.complete(spent, ARGS).catch(() => undefined);
      }
      expect(r.breakerOpen()).toBe(false);
   });
});

describe("LlmRunner cache", () => {
   it("serves a repeat from cache without calling the provider", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(scripted([ok("a")], seen), SETTINGS, fakeClock());
      const first = await r.complete(null, { ...ARGS, cacheKey: "k1" });
      const second = await r.complete(null, { ...ARGS, cacheKey: "k1" });
      expect(first.cached).toBe(false);
      expect(second.cached).toBe(true);
      expect(second.text).toBe("a");
      expect(seen).toHaveLength(1);
      expect(r.totals.cacheHits).toBe(1);
   });

   it("skips the cache when there is no key or it is switched off", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(scripted([ok("a")], seen), SETTINGS, fakeClock());
      await r.complete(null, ARGS);
      await r.complete(null, ARGS);
      await r.complete(null, { ...ARGS, cacheKey: "k", useCache: false });
      await r.complete(null, { ...ARGS, cacheKey: "k", useCache: false });
      expect(seen).toHaveLength(4);
   });

   it("evicts the least recently used entry", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(scripted([ok("a")], seen), SETTINGS, fakeClock());
      await r.complete(null, { ...ARGS, cacheKey: "a" });
      await r.complete(null, { ...ARGS, cacheKey: "b" });
      await r.complete(null, { ...ARGS, cacheKey: "a" }); // touch a
      await r.complete(null, { ...ARGS, cacheKey: "c" }); // evicts b (max 2)
      const before = seen.length;
      await r.complete(null, { ...ARGS, cacheKey: "a" });
      expect(seen.length).toBe(before); // a survived
      await r.complete(null, { ...ARGS, cacheKey: "b" });
      expect(seen.length).toBe(before + 1); // b was evicted
   });

   it("never caches a failure", async () => {
      const seen: LlmRequest[] = [];
      const r = new LlmRunner(
         scripted([new LlmError("no", "http", false, 400), ok("a")], seen),
         SETTINGS,
         fakeClock(),
      );
      await r.complete(null, { ...ARGS, cacheKey: "k" }).catch(() => undefined);
      const out = await r.complete(null, { ...ARGS, cacheKey: "k" });
      expect(out.cached).toBe(false);
      expect(out.text).toBe("a");
   });
});

describe("LlmRunner concurrency", () => {
   it("never has more calls in flight than the cap", async () => {
      let inFlight = 0;
      let peak = 0;
      const provider: LlmProvider = {
         id: "fake",
         async complete() {
            inFlight++;
            peak = Math.max(peak, inFlight);
            await new Promise((r) => setTimeout(r, 5));
            inFlight--;
            return ok("a");
         },
      };
      const r = new LlmRunner(
         provider,
         { ...SETTINGS, concurrency: 2 },
         fakeClock(),
      );
      await Promise.all(
         Array.from({ length: 8 }, () => r.complete(null, ARGS)),
      );
      expect(peak).toBe(2);
   });

   it("frees the slot on failure so later calls still run", async () => {
      let n = 0;
      const provider: LlmProvider = {
         id: "fake",
         async complete() {
            if (n++ < 3) throw new LlmError("no", "http", false, 400);
            return ok("a");
         },
      };
      const r = new LlmRunner(
         provider,
         {
            ...SETTINGS,
            concurrency: 1,
            maxAttempts: 1,
            breaker: { failures: 100, cooldownMs: 1 },
         },
         fakeClock(),
      );
      const results = await Promise.allSettled(
         Array.from({ length: 5 }, () => r.complete(null, ARGS)),
      );
      expect(results.filter((x) => x.status === "fulfilled")).toHaveLength(2);
   });
});
