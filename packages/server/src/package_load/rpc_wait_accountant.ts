// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Per-load "wait" accounting for the package-load worker.
 *
 * Tracks the wall-clock during which at least one proxied schema fetch is
 * outstanding (the union, never the sum), so a load can report
 * `compile = compileRegion - schemaFetchWait`. Not a CPU counter, because
 * `process.cpuUsage()` is process-wide across workers; the result is a
 * ceiling on compile CPU, since GC and other stalls also land in it.
 *
 * Scoped by `jobId`, so a straggling fetch from a failed earlier load on a
 * reused worker cannot corrupt the next load's numbers. Assumes one load per
 * worker at a time.
 */
export class RpcWaitAccountant {
   private inFlight = 0;
   private waitStartMs = 0;
   private accumMs = 0;
   private startedCount = 0;
   private activeJobId: string | undefined;

   constructor(private readonly now: () => number = () => performance.now()) {}

   /** Begin accounting for `jobId`, discarding any prior-load state. */
   begin(jobId: string): void {
      this.activeJobId = jobId;
      this.inFlight = 0;
      this.waitStartMs = 0;
      this.accumMs = 0;
      this.startedCount = 0;
   }

   /** A proxied fetch for `jobId` was issued. Ignored if `jobId` isn't active. */
   noteStart(jobId: string): void {
      if (jobId !== this.activeJobId) return;
      if (this.inFlight === 0) this.waitStartMs = this.now();
      this.inFlight += 1;
      this.startedCount += 1;
   }

   /** A proxied fetch for `jobId` settled. Ignored if `jobId` isn't active. */
   noteSettle(jobId: string): void {
      if (jobId !== this.activeJobId) return;
      this.inFlight -= 1;
      if (this.inFlight === 0) this.accumMs += this.now() - this.waitStartMs;
   }

   /** Accumulated wall-time with >=1 active-load fetch outstanding (union). */
   get waitMs(): number {
      return this.accumMs;
   }

   /** Number of proxied fetches started for the active load. */
   get fetches(): number {
      return this.startedCount;
   }
}
