/**
 * Per-load "wait" accounting for the package-load worker.
 *
 * A package load spends its time in two very different ways: CPU-bound Malloy
 * compilation, and I/O waiting on connection schema fetches that are proxied
 * back to the main thread. Today only the *total* load time is observable, so
 * an operator seeing slow loads can't tell which half dominates. This tracks
 * the wall-clock during which at least one proxied schema fetch is outstanding,
 * so the load handler can report `compile = compileRegion - schemaFetchWait`
 * alongside the fetch time itself.
 *
 * Why wall-minus-wait and not a CPU counter: worker threads share the host
 * process, so `process.cpuUsage()` is process-wide and would fold in every
 * other worker's CPU — useless for a per-load number. Wall-minus-wait is the
 * honest per-load proxy. Treat the compute figure as a conservative *ceiling*
 * on compile CPU: only proxied waits are subtracted, so any non-fetch stall
 * (GC, scheduler preemption) also lands in "compute".
 *
 * Overlapping fetches are counted as their union (the wall-time with >=1
 * outstanding), never the sum, so concurrent fetches during one compile don't
 * over-subtract.
 *
 * Scoped by `jobId`: the pool runs one load per worker at a time, but reuses a
 * worker across loads, and a load that fails can leave a proxied fetch in
 * flight. Gating every start/settle on the active job makes such a straggler's
 * late response a no-op instead of corrupting the next load's numbers. (If the
 * one-load-per-worker rule ever changes, this stays correct per job but the
 * compute ceiling would then also fold in a sibling load's CPU.)
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

   /**
    * A proxied fetch for `jobId` settled (resolved or rejected). Ignored if
    * `jobId` isn't active, so a straggler from a prior reused-worker load
    * can't drive the counter negative or close the active load's bracket.
    */
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
