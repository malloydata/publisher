/**
 * Evaluate a 5-field UNIX cron expression, in UTC, for the standalone
 * materialization scheduler. Mirrors the control plane's `CronEvaluator`
 * semantics (5-field, UTC, "strictly after") so a given `materialization.schedule`
 * fires at the same instants whether the publisher runs standalone or the control
 * plane drives it.
 *
 * Thin wrapper over `cron-parser` that pins two policies:
 *   - **exactly 5 fields** — `cron-parser` also accepts an optional leading
 *     seconds field (6 fields); the manifest contract is 5-field UNIX cron, so a
 *     6-field expression is rejected rather than silently reinterpreted.
 *   - **UTC** — schedules are absolute, never tied to the server's local zone.
 */
import { CronExpressionParser } from "cron-parser";

export class CronEvaluator {
   /** True when `expr` is a syntactically valid 5-field UNIX cron. */
   isValid(expr: string): boolean {
      if (!this.hasFiveFields(expr)) {
         return false;
      }
      try {
         CronExpressionParser.parse(expr, { tz: "UTC" });
         return true;
      } catch {
         return false;
      }
   }

   /**
    * The next fire instant strictly after `from` (UTC). Throws when `expr` is
    * not a valid 5-field cron — callers guard with {@link isValid} (the
    * scheduler skips + logs an invalid cron rather than letting it throw in the
    * tick).
    */
   nextAfter(expr: string, from: Date): Date {
      if (!this.hasFiveFields(expr)) {
         throw new Error(
            `Expected a 5-field UNIX cron, got: ${JSON.stringify(expr)}`,
         );
      }
      return CronExpressionParser.parse(expr, {
         currentDate: from,
         tz: "UTC",
      })
         .next()
         .toDate();
   }

   /** Whitespace-split field count is exactly 5. */
   private hasFiveFields(expr: string): boolean {
      return typeof expr === "string" && expr.trim().split(/\s+/).length === 5;
   }
}
