// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "./logger";

let installed: ((reason: unknown) => void) | undefined;

/**
 * A process-level backstop: an unhandled promise rejection is logged at error
 * and the process keeps serving.
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
 * Idempotent: called again, it installs nothing new. Returns a function that
 * removes the listener, for tests. No metric is recorded yet; the log line is
 * the only signal.
 */
export function installProcessSafetyHandlers(): () => void {
   if (!installed) {
      const listener = (reason: unknown): void => {
         const error = reason instanceof Error ? reason : undefined;
         logger.error("Unhandled promise rejection (process kept running)", {
            message: error ? error.message : String(reason),
            stack: error?.stack,
         });
      };
      process.on("unhandledRejection", listener);
      installed = listener;
   }
   return () => {
      if (installed) process.off("unhandledRejection", installed);
      installed = undefined;
   };
}
