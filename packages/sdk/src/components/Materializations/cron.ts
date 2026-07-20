import { CronExpressionParser } from "cron-parser";
import cronstrue from "cronstrue";

export interface CronInfo {
   valid: boolean;
   /** Human-readable summary, e.g. "Every minute" (empty when invalid). */
   description: string;
   /** Next fire instant in UTC (null when invalid). */
   nextRun: Date | null;
   /** Reason the expression was rejected (set only when `valid` is false). */
   error?: string;
}

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

/**
 * Reject cron-parser / Quartz extensions absent from plain UNIX cron (`L`, `W`,
 * `#`, `?`). Mirrors the server's `CronEvaluator.isPlainUnixGrammar` so the UI
 * preview flags the same expressions the server (and the control plane) reject,
 * rather than describing a cron that would silently never arm after publish.
 */
function isPlainUnixGrammar(expr: string): boolean {
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

/**
 * Describe a 5-field UNIX cron for display: a human-readable summary (cronstrue)
 * and the next fire instant (cron-parser, in UTC — matching the publisher's
 * server-side scheduler so the UI shows the same instants it will fire at).
 *
 * The expression is required to be exactly 5 fields and plain UNIX grammar,
 * mirroring the server's `CronEvaluator`: cron-parser also accepts an optional
 * leading seconds field (6 fields) and extensions (L/W/#/?), but the manifest
 * contract is plain 5-field UNIX cron, so those are reported invalid rather
 * than silently reinterpreted (or accepted here yet rejected after publish).
 */
export function describeCron(expr: string): CronInfo {
   const trimmed = (expr ?? "").trim();
   if (trimmed.split(/\s+/).filter(Boolean).length !== 5) {
      return {
         valid: false,
         description: "",
         nextRun: null,
         error: "Expected a 5-field UNIX cron: minute hour day-of-month month day-of-week.",
      };
   }
   if (!isPlainUnixGrammar(trimmed)) {
      return {
         valid: false,
         description: "",
         nextRun: null,
         error: "Unsupported cron syntax: L, W, #, and ? extensions are not allowed (plain 5-field UNIX cron only).",
      };
   }
   try {
      const description = cronstrue.toString(trimmed, {
         use24HourTimeFormat: true,
         verbose: false,
      });
      const nextRun = CronExpressionParser.parse(trimmed, { tz: "UTC" })
         .next()
         .toDate();
      return { valid: true, description, nextRun };
   } catch (e) {
      return {
         valid: false,
         description: "",
         nextRun: null,
         error: e instanceof Error ? e.message : "Invalid cron expression.",
      };
   }
}

/** Absolute UTC label for a next-run instant, e.g. "Jul 14, 2026, 06:00 UTC". */
export function formatNextRun(d: Date | null): string {
   if (!d) return "—";
   return (
      d.toLocaleString(undefined, {
         year: "numeric",
         month: "short",
         day: "numeric",
         hour: "2-digit",
         minute: "2-digit",
         timeZone: "UTC",
         hour12: false,
      }) + " UTC"
   );
}
