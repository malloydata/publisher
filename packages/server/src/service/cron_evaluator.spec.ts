import { describe, expect, it } from "bun:test";
import { CronEvaluator } from "./cron_evaluator";

describe("CronEvaluator", () => {
   const cron = new CronEvaluator();

   describe("isValid", () => {
      it("accepts 5-field UNIX crons", () => {
         expect(cron.isValid("0 6 * * *")).toBe(true);
         expect(cron.isValid("*/15 * * * *")).toBe(true);
         expect(cron.isValid("0 0 1 1 *")).toBe(true);
      });

      it("rejects a 6-field (seconds) cron — the contract is 5-field", () => {
         expect(cron.isValid("0 0 6 * * *")).toBe(false);
      });

      it("rejects garbage and empty input", () => {
         expect(cron.isValid("not a cron")).toBe(false);
         expect(cron.isValid("")).toBe(false);
         expect(cron.isValid("* * *")).toBe(false);
         // Out-of-range field.
         expect(cron.isValid("99 * * * *")).toBe(false);
      });
   });

   describe("nextAfter (UTC, strictly after)", () => {
      it("returns the next daily fire", () => {
         const from = new Date("2026-07-13T00:00:00Z");
         expect(cron.nextAfter("0 6 * * *", from).toISOString()).toBe(
            "2026-07-13T06:00:00.000Z",
         );
      });

      it("rolls to the next day when the fire time has passed", () => {
         const from = new Date("2026-07-13T07:00:00Z");
         expect(cron.nextAfter("0 6 * * *", from).toISOString()).toBe(
            "2026-07-14T06:00:00.000Z",
         );
      });

      it("advances strictly past a `from` that lands exactly on a fire", () => {
         const from = new Date("2026-07-13T06:00:00Z");
         // Strictly after: the 06:00 instant itself is excluded.
         expect(cron.nextAfter("0 6 * * *", from).toISOString()).toBe(
            "2026-07-14T06:00:00.000Z",
         );
      });

      it("steps every 15 minutes", () => {
         const from = new Date("2026-07-13T09:07:00Z");
         expect(cron.nextAfter("*/15 * * * *", from).toISOString()).toBe(
            "2026-07-13T09:15:00.000Z",
         );
      });

      it("throws on an invalid cron (callers guard with isValid)", () => {
         expect(() => cron.nextAfter("nope", new Date())).toThrow();
      });
   });
});
