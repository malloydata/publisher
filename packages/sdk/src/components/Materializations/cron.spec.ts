// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { describeCron, formatNextRun } from "./cron";

/**
 * `describeCron` is the only thing standing between a typed cron and a
 * schedule that publishes and then never fires. It drives four things at once
 * in `SetScheduleDialog` — the error state, the helper text, whether Save is
 * enabled, and the section's summary line — and its whole job is to agree with
 * the server's `CronEvaluator` about what counts as a cron. Agreeing is not
 * something a screenshot can show, which is why it is asserted here.
 */
describe("describeCron: what it accepts", () => {
   it("describes a valid five-field expression and says when it next fires", () => {
      const info = describeCron("0 6 * * *");
      expect(info.valid).toBe(true);
      expect(info.description).toContain("06:00");
      expect(info.nextRun).toBeInstanceOf(Date);
      expect(info.error).toBeUndefined();
   });

   it("takes the three-letter month and day names UNIX cron defines", () => {
      for (const expr of ["0 0 1 JAN *", "0 0 * * MON", "0 0 * * mon-fri"]) {
         expect(describeCron(expr).valid).toBe(true);
      }
   });

   it("takes lists, ranges and steps", () => {
      for (const expr of ["0,30 * * * *", "0 9-17 * * *", "*/15 * * * *"]) {
         expect(describeCron(expr).valid).toBe(true);
      }
   });

   it("ignores surrounding and repeated whitespace", () => {
      expect(describeCron("  0   6  *  *  *  ").valid).toBe(true);
   });

   it("computes the next run in UTC, not in the machine's zone", () => {
      // A daily 06:00 job fires at 06:00 UTC wherever the browser is. Reading
      // it back in UTC is the only way to assert that without the suite
      // passing or failing by timezone.
      const next = describeCron("0 6 * * *").nextRun;
      expect(next?.getUTCHours()).toBe(6);
      expect(next?.getUTCMinutes()).toBe(0);
   });
});

describe("describeCron: what it refuses", () => {
   it("refuses anything that is not five fields", () => {
      // Six fields is the trap: cron-parser reads a leading SECONDS field, so
      // "0 0 6 * * *" parses happily and means something else entirely. The
      // manifest contract is five fields, so it has to be refused here rather
      // than silently reinterpreted.
      for (const expr of ["", "   ", "0 6 * *", "0 0 6 * * *"]) {
         const info = describeCron(expr);
         expect(info.valid).toBe(false);
         expect(info.error).toContain("5-field");
         expect(info.nextRun).toBeNull();
         expect(info.description).toBe("");
      }
   });

   it("refuses the Quartz extensions the server does not implement", () => {
      // Each of these parses in cron-parser and would describe itself
      // plausibly, then never arm after publish.
      for (const expr of [
         "0 0 L * *",
         "0 0 15W * *",
         "0 0 * * 6#3",
         "0 0 * * ?",
      ]) {
         const info = describeCron(expr);
         expect(info.valid).toBe(false);
         expect(info.error).toContain("Unsupported cron syntax");
      }
   });

   it("refuses a word that is not a month or a day", () => {
      const info = describeCron("0 0 * * FUNDAY");
      expect(info.valid).toBe(false);
      expect(info.error).toContain("Unsupported cron syntax");
   });

   it("refuses an out-of-range field with the parser's own reason", () => {
      const info = describeCron("0 99 * * *");
      expect(info.valid).toBe(false);
      expect(info.error).toBeTruthy();
      expect(info.nextRun).toBeNull();
   });

   it("treats a null or undefined expression as empty rather than throwing", () => {
      for (const expr of [null, undefined]) {
         const info = describeCron(expr as unknown as string);
         expect(info.valid).toBe(false);
         expect(info.error).toContain("5-field");
      }
   });
});

describe("formatNextRun", () => {
   it("renders an instant in UTC and says so", () => {
      const label = formatNextRun(new Date("2026-07-14T06:00:00Z"));
      expect(label).toContain("2026");
      expect(label).toContain("06:00");
      expect(label.endsWith(" UTC")).toBe(true);
   });

   it("renders an em dash when there is no next run", () => {
      expect(formatNextRun(null)).toBe("—");
   });
});
