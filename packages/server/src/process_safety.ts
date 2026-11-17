// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "./logger";
import { lazyCounter } from "./telemetry";

const unhandledRejections = lazyCounter(
   "publisher_unhandled_rejection_total",
   "Unhandled promise rejections caught by the process-level backstop (logged, process kept running), including those from package-load workers.",
);

/** What the backstop reports of a rejection: never throws to build. */
export interface RejectionReport {
   message: string;
   /** Present only when the reason carried a string stack. */
   stack?: string;
}

/**
 * Describe a rejection reason for the log. A reason is arbitrary: a null
 * prototype, a throwing `toString`, a getter that throws on `message` or
 * `stack`, a Proxy that throws on `getPrototypeOf`. Every read is guarded and
 * falls back, so the report itself cannot throw.
 */
export function describeRejection(reason: unknown): RejectionReport {
   let message: string | undefined;
   let stack: string | undefined;
   try {
      if (reason instanceof Error) {
         message = String(reason.message);
         if (typeof reason.stack === "string") stack = reason.stack;
      }
   } catch {
      // Fall through to the fallbacks below.
   }
   if (message === undefined) {
      try {
         message = String(reason);
      } catch {
         try {
            message = Object.prototype.toString.call(reason);
         } catch {
            message = "[unprintable rejection reason]";
         }
      }
   }
   return stack === undefined ? { message } : { message, stack };
}

/**
 * Count and log one unhandled rejection. Total: nothing in it throws, and the
 * counter goes first so a failure to build the log line cannot lose the count.
 * Also what the pool runs for a rejection a worker reports.
 */
export function recordUnhandledRejection(
   report: RejectionReport,
   extra: Record<string, unknown> = {},
): void {
   try {
      unhandledRejections().add(1);
   } catch {
      // A broken meter must not stop the log line.
   }
   try {
      logger.error("Unhandled promise rejection (process kept running)", {
         ...extra,
         message: report.message,
         stack: report.stack,
      });
   } catch {
      // Nothing left to do; the process must keep running.
   }
}

let installed: ((reason: unknown) => void) | undefined;

/**
 * A process-level backstop: an unhandled promise rejection is logged at error
 * and counted, and the process keeps serving.
 *
 * Node's default is to crash on one. The ones this catches come from
 * dependencies, which Publisher cannot guard at a call site: Malloy's BigQuery
 * connection cancels a job on abort with a fire-and-forget `job.cancel()`, so a
 * failed cancel rejects with nobody listening
 * (https://github.com/malloydata/malloy/issues/3232), and Publisher aborts
 * warehouse reads on a materialization cancel and on a query timeout.
 *
 * `uncaughtException` is deliberately NOT handled here. A synchronous throw
 * can leave state half-written, so crashing and restarting is still the right
 * answer there.
 *
 * Idempotent: called again, it installs nothing new. The returned function
 * removes the listener only when THIS call installed it. A package-load worker
 * passes `report` to forward the rejection to the main thread, which logs and
 * counts it there; without it the rejection is recorded in place.
 */
export function installProcessSafetyHandlers(
   report: (report: RejectionReport) => void = recordUnhandledRejection,
): () => void {
   if (installed) return () => {};
   const listener = (reason: unknown): void => {
      try {
         report(describeRejection(reason));
      } catch {
         // The backstop must never throw.
      }
   };
   process.on("unhandledRejection", listener);
   installed = listener;
   return () => {
      if (installed !== listener) return;
      process.off("unhandledRejection", listener);
      installed = undefined;
   };
}
