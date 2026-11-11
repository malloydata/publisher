// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { FailureCooldown, ProviderCooldownError } from "./cooldown";

function cooldown(threshold = 3, windowMs = 60_000) {
   const clock = { now: 1_000 };
   return {
      clock,
      gate: new FailureCooldown("openai", threshold, windowMs, () => clock.now),
   };
}

describe("FailureCooldown", () => {
   it("stays open below the threshold", () => {
      const { gate } = cooldown();
      gate.failure();
      gate.failure();
      expect(() => gate.check()).not.toThrow();
   });

   it("refuses for the window once the threshold is reached, naming the wait", () => {
      const { gate, clock } = cooldown(3, 60_000);
      for (let i = 0; i < 3; i++) gate.failure();
      clock.now += 1_500;
      let thrown: unknown;
      try {
         gate.check();
      } catch (e) {
         thrown = e;
      }
      expect(thrown).toBeInstanceOf(ProviderCooldownError);
      const error = thrown as ProviderCooldownError;
      expect(error.provider).toBe("openai");
      expect(error.retryInMs).toBe(58_500);
      expect(error.message).toBe(
         "openai is cooling down after repeated failures; retrying in 59s",
      );
   });

   it("reopens when the window has passed", () => {
      const { gate, clock } = cooldown(1, 10_000);
      gate.failure();
      expect(() => gate.check()).toThrow(ProviderCooldownError);
      clock.now += 10_000;
      expect(() => gate.check()).not.toThrow();
   });

   it("resets the run of failures on a success", () => {
      const { gate } = cooldown(2);
      gate.failure();
      gate.success();
      gate.failure();
      expect(() => gate.check()).not.toThrow();
   });

   it("starts a fresh count after tripping, so one more failure does not re-trip", () => {
      const { gate, clock } = cooldown(2, 1_000);
      gate.failure();
      gate.failure();
      clock.now += 1_000;
      gate.failure();
      expect(() => gate.check()).not.toThrow();
   });
});
