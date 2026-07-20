import { describe, expect, it } from "bun:test";
import { summarizeRenderLogs } from "./renderLogs";

describe("summarizeRenderLogs", () => {
   it("returns nothing when there are no logs", () => {
      expect(summarizeRenderLogs(undefined)).toBeUndefined();
      expect(summarizeRenderLogs([])).toBeUndefined();
   });

   it("surfaces a warning, which is the severity a bad render tag reports", () => {
      expect(
         summarizeRenderLogs([
            {
               severity: "warn",
               message: "Unknown render tag 'viz.stack.y' on field 'root'",
            },
         ]),
      ).toEqual({
         severity: "warn",
         title: "Unknown render tag 'viz.stack.y' on field 'root'",
      });
   });

   it("reports the worst severity when both are present", () => {
      const summary = summarizeRenderLogs([
         { severity: "warn", message: "first" },
         { severity: "error", message: "second" },
      ]);
      expect(summary?.severity).toBe("error");
      expect(summary?.title).toBe("first\nsecond");
   });

   it("drops debug and info, which are not worth interrupting for", () => {
      expect(
         summarizeRenderLogs([
            { severity: "debug", message: "noise" },
            { severity: "info", message: "more noise" },
         ]),
      ).toBeUndefined();
   });

   it("drops entries with no message or no severity rather than showing an empty tooltip", () => {
      // Every LogMessage field is optional in the generated client.
      expect(summarizeRenderLogs([{ severity: "warn" }])).toBeUndefined();
      expect(summarizeRenderLogs([{ message: "orphan" }])).toBeUndefined();
   });

   it("dedupes repeated messages", () => {
      const summary = summarizeRenderLogs([
         { severity: "warn", message: "same" },
         { severity: "warn", message: "same" },
      ]);
      expect(summary?.title).toBe("same");
   });
});
