import * as fs from "fs";

import { logger } from "../logger";
import type { PackageMemoryGovernor } from "./package_memory_governor";

const DEFAULT_INTERVAL_MS = 30_000;

interface LinuxProcStatus {
   threads?: number;
   vmRssBytes?: number;
   vmSizeBytes?: number;
   vmPeakBytes?: number;
   vmDataBytes?: number;
   voluntaryCtxSwitches?: number;
   nonvoluntaryCtxSwitches?: number;
}

/**
 * Parse the subset of `/proc/self/status` that matters for diagnosing
 * thread / virtual-memory leaks. The file is small (<5KB), so reading
 * it synchronously here is cheap and avoids fs-promise queueing.
 *
 * Format is `Key:\t<value> [unit]` per line. Sizes are reported in kB;
 * we normalize to bytes so log output matches `process.memoryUsage()`.
 */
function readLinuxProcStatus(): LinuxProcStatus | null {
   try {
      const raw = fs.readFileSync("/proc/self/status", "utf8");
      const out: LinuxProcStatus = {};
      for (const line of raw.split("\n")) {
         const [keyRaw, valueRaw] = line.split(":");
         if (!keyRaw || !valueRaw) continue;
         const key = keyRaw.trim();
         const value = valueRaw.trim();
         switch (key) {
            case "Threads":
               out.threads = Number(value);
               break;
            case "VmRSS":
               out.vmRssBytes = kBToBytes(value);
               break;
            case "VmSize":
               out.vmSizeBytes = kBToBytes(value);
               break;
            case "VmPeak":
               out.vmPeakBytes = kBToBytes(value);
               break;
            case "VmData":
               out.vmDataBytes = kBToBytes(value);
               break;
            case "voluntary_ctxt_switches":
               out.voluntaryCtxSwitches = Number(value);
               break;
            case "nonvoluntary_ctxt_switches":
               out.nonvoluntaryCtxSwitches = Number(value);
               break;
         }
      }
      return out;
   } catch {
      return null;
   }
}

function kBToBytes(value: string): number | undefined {
   const num = Number(value.replace(/\s*kB$/, ""));
   if (!Number.isFinite(num)) return undefined;
   return num * 1024;
}

/**
 * Bun exposes JSC heap stats via the `bun:jsc` builtin. Optional —
 * absent under plain Node — and best-effort: failures are swallowed
 * so the reporter never crashes the process.
 */
async function readBunJscStats(): Promise<Record<string, number> | null> {
   if (typeof (globalThis as { Bun?: unknown }).Bun === "undefined") {
      return null;
   }
   try {
      // Dynamic import so Node builds don't fail at parse time.
      const jsc = (await import("bun:jsc")) as unknown as {
         heapStats?: () => Record<string, number>;
         memoryUsage?: () => Record<string, number>;
      };
      const heap = jsc.heapStats?.();
      const mem = jsc.memoryUsage?.();
      if (!heap && !mem) return null;
      return { ...(heap ?? {}), ...(mem ?? {}) };
   } catch {
      return null;
   }
}

/**
 * Periodically logs process memory and thread counts to give ops a
 * cheap, always-on signal for the leak classes that have OOM-killed
 * prod (DuckDB connection thread pools, libuv worker pool, Malloy
 * compile heap, etc.).
 *
 * Logs at `info` so it shows up without flipping `LOG_LEVEL`. Volume
 * is low (~2 lines/minute by default). Pulls the memory governor's
 * snapshot too so RSS/back-pressure state appears in the same line as
 * Node/Bun heap.
 */
export class ProcessStatsReporter {
   private timer: ReturnType<typeof setInterval> | null = null;
   private readonly intervalMs: number;
   private readonly memoryGovernor: PackageMemoryGovernor | null;

   constructor(
      memoryGovernor: PackageMemoryGovernor | null,
      intervalMs: number = DEFAULT_INTERVAL_MS,
   ) {
      this.memoryGovernor = memoryGovernor;
      this.intervalMs = intervalMs;
   }

   public start(): void {
      if (this.timer !== null) return;
      // Immediate first sample so a freshly-started pod logs its
      // baseline before the first 30s has elapsed.
      void this.tick();
      this.timer = setInterval(() => void this.tick(), this.intervalMs);
      // Don't keep the event loop alive on our account — if everything
      // else has shut down, the reporter shouldn't block exit.
      (
         this.timer as ReturnType<typeof setInterval> & {
            unref?: () => void;
         }
      ).unref?.();
      logger.info(
         `ProcessStatsReporter started (intervalMs=${this.intervalMs})`,
      );
   }

   public stop(): void {
      if (this.timer !== null) {
         clearInterval(this.timer);
         this.timer = null;
      }
   }

   private async tick(): Promise<void> {
      try {
         const mem = process.memoryUsage();
         const proc =
            process.platform === "linux" ? readLinuxProcStatus() : null;
         const bun = await readBunJscStats();
         const governor = this.memoryGovernor?.getStatus() ?? null;

         logger.info("process stats", {
            uptimeSeconds: Math.round(process.uptime()),
            nodeMemory: {
               rssBytes: mem.rss,
               heapTotalBytes: mem.heapTotal,
               heapUsedBytes: mem.heapUsed,
               externalBytes: mem.external,
               arrayBuffersBytes: mem.arrayBuffers,
            },
            linux: proc,
            bunJsc: bun,
            memoryGovernor: governor,
         });
      } catch (err) {
         logger.warn("ProcessStatsReporter tick failed", { error: err });
      }
   }
}
