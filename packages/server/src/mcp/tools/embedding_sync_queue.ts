// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../../logger";

/**
 * Runs jobs one at a time, in the order they were queued.
 *
 * The embedding provider and its rate limit belong to the whole process, not
 * to a package. Several packages syncing at once would split one rate limit
 * between them, and a restart loads every package at nearly the same moment.
 * So all embedding syncs go through one queue, and a package's sync starts
 * when the one before it has finished.
 *
 * A job that throws is logged and does not stop the jobs behind it.
 */
export class SerialQueue {
   private tail: Promise<void> = Promise.resolve();
   private waiting = 0;

   /** Queue `job`. The returned promise settles when the job has finished. */
   enqueue(job: () => Promise<void>): Promise<void> {
      this.waiting++;
      const run = this.tail
         .then(job)
         .catch((error: unknown) => {
            logger.warn("[Embedding sync] A queued sync job failed", {
               error: error instanceof Error ? error.message : String(error),
            });
         })
         .finally(() => {
            this.waiting--;
         });
      this.tail = run;
      return run;
   }

   /** Jobs queued or running. */
   get size(): number {
      return this.waiting;
   }

   /** Settles when everything queued so far has finished. */
   idle(): Promise<void> {
      return this.tail;
   }
}

/** The one queue every embedding sync in this process goes through. */
export const embeddingSyncQueue = new SerialQueue();
