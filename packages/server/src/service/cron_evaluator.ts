/**
 * Evaluate a 5-field UNIX cron expression, in UTC, for the standalone
 * materialization scheduler. Mirrors the control plane's `CronEvaluator`
 * semantics (5-field, UTC, "strictly after") so a given `materialization.schedule`
 * fires at the same instants whether the publisher runs standalone or the control
 * plane drives it.
 *
 * Thin wrapper over `cron-parser` that pins three policies:
 *   - **exactly 5 fields** — `cron-parser` also accepts an optional leading
 *     seconds field (6 fields); the manifest contract is 5-field UNIX cron, so a
 *     6-field expression is rejected rather than silently reinterpreted.
 *   - **plain UNIX grammar** — `cron-parser` accepts extensions (`L` last,
 *     `W` nearest-weekday, `#` nth-weekday, `?` Quartz no-specific) that the
 *     control plane's `cron-utils` UNIX parser rejects. Without this pin, an
 *     expression like `0 6 L * *` would validate and fire locally but, after
 *     publish, silently never arm in production. Month/day names (JAN, MON-FRI)
 *     are still allowed.
 *   - **UTC** — schedules are absolute, never tied to the server's local zone.
 */
import { CronExpressionParser } from "cron-parser";

/** Valid 3-letter month and day-of-week names in UNIX cron. */
const UNIX_CRON_NAMES = new Set([
   "JAN",
   "FEB",
   "MAR",
   "APR",
   "MAY",
   "JUN",
   "JUL",
   "AUG",
   "SEP",
   "OCT",
   "NOV",
   "DEC",
   "SUN",
   "MON",
   "TUE",
   "WED",
   "THU",
   "FRI",
   "SAT",
]);

export class CronEvaluator {
   /** True when `expr` is a syntactically valid 5-field UNIX cron. */
   isValid(expr: string): boolean {
      if (!this.hasFiveFields(expr) || !this.isPlainUnixGrammar(expr)) {
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
      if (!this.hasFiveFields(expr) || !this.isPlainUnixGrammar(expr)) {
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

   /**
    * Reject cron-parser / Quartz extensions absent from plain UNIX cron: `#`
    * and `?` (never part of a name), and any alphabetic token that is not a
    * UNIX month/day name — which catches `L`, `W`, `LW`, `L-3`, `15W`, etc.
    * while still allowing named fields like `JUL` or `MON-FRI`.
    */
   private isPlainUnixGrammar(expr: string): boolean {
      for (const field of expr.trim().split(/\s+/)) {
         if (/[#?]/.test(field)) return false;
         for (const token of field.split(/[,\-/]/)) {
            if (token === "" || token === "*") continue;
            if (
               /[A-Za-z]/.test(token) &&
               !UNIX_CRON_NAMES.has(token.toUpperCase())
            ) {
               return false;
            }
         }
      }
      return true;
   }
}
