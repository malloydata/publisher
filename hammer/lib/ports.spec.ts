// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import net from "node:net";
import {
   HOST,
   isPortFree,
   PortAllocator,
   portsReportedInUse,
   portsTakenAtExit,
} from "./ports";
import { waitFor } from "./util";

const held: net.Server[] = [];

function hold(port: number): Promise<net.Server> {
   return new Promise((resolve, reject) => {
      const s = net.createServer();
      s.once("error", reject);
      s.listen({ port, host: HOST, exclusive: true }, () => {
         held.push(s);
         resolve(s);
      });
   });
}

afterEach(async () => {
   await Promise.all(
      held.splice(0).map((s) => new Promise<void>((r) => s.close(() => r()))),
   );
});

describe("PortAllocator from the OS", () => {
   it("hands out distinct ports that are bindable", async () => {
      const a = new PortAllocator();
      const ports = await a.allocate(6);
      expect(new Set(ports).size).toBe(6);
      for (const p of ports) {
         expect(p).toBeGreaterThan(0);
         expect(await isPortFree(p)).toBe(true);
      }
   });

   it("never hands out a port twice until it is released", async () => {
      const a = new PortAllocator();
      const seen = new Set<number>();
      for (let i = 0; i < 20; i++) {
         for (const p of await a.allocate(2)) {
            expect(seen.has(p)).toBe(false);
            seen.add(p);
         }
      }
      expect(a.inUse.size).toBe(40);
      a.release(seen);
      expect(a.inUse.size).toBe(0);
   });

   it("two allocators (two runs) get different ports while both hold them", async () => {
      const a = new PortAllocator();
      const b = new PortAllocator();
      const pa = await a.allocate(4);
      // A run's server binds what it was given; the other run must step past it.
      for (const p of pa) await hold(p);
      const pb = await b.allocate(4);
      for (const p of pb) expect(pa).not.toContain(p);
   });
});

describe("PortAllocator from a base (--port)", () => {
   it("walks upward from the base, skipping ports the probe reports busy", async () => {
      const busy = new Set([20001, 20003]);
      const a = new PortAllocator(HOST, async (p) => !busy.has(p));
      expect(await a.allocate(3, 20000)).toEqual([20000, 20002, 20004]);
      // The run's own ports are skipped without asking the OS.
      expect(await a.allocate(1, 20000)).toEqual([20005]);
   });

   it("lets REST and MCP share one base without overlapping", async () => {
      const a = new PortAllocator(HOST, async () => true);
      const [rest] = await a.allocate(1, 30000);
      const [mcp] = await a.allocate(1, 30000);
      expect(rest).toBe(30000);
      expect(mcp).toBe(30001);
   });

   it("reuses a released port", async () => {
      const a = new PortAllocator(HOST, async () => true);
      const [p] = await a.allocate(1, 31000);
      a.release([p]);
      expect(await a.allocate(1, 31000)).toEqual([p]);
   });

   it("steps over a port another process really holds", async () => {
      const [taken] = await new PortAllocator().allocate(1);
      await hold(taken);
      const a = new PortAllocator();
      const [p] = await a.allocate(1, taken);
      expect(p).not.toBe(taken);
      expect(p).toBeGreaterThan(taken);
   });

   it("rejects an invalid base", async () => {
      const a = new PortAllocator(HOST, async () => true);
      await expect(a.allocate(1, 0)).rejects.toThrow("invalid base port");
      await expect(a.allocate(1, 70000)).rejects.toThrow("invalid base port");
   });
});

describe("isPortFree / portsReportedInUse", () => {
   it("reports a held port as busy", async () => {
      const [p] = await new PortAllocator().allocate(1);
      expect(await isPortFree(p)).toBe(true);
      await hold(p);
      expect(await isPortFree(p)).toBe(false);
   });

   it("names the ports Publisher reported in use", () => {
      const log =
         "info: Publisher server listening at http://127.0.0.1:50236\n" +
         "error: Port 50237 in use; pass --mcp_port <n>\n";
      expect(portsReportedInUse(log)).toEqual([50237]);
      expect(portsReportedInUse("listen EADDRINUSE")).toEqual([]);
   });
});

describe("portsTakenAtExit", () => {
   const never = async (): Promise<boolean> => {
      throw new Error("must not probe when Publisher named the port");
   };

   it("is the named port when it is one of ours", async () => {
      const out = "error: Port 50237 in use; pass --mcp_port <n>\n";
      expect(await portsTakenAtExit(out, [50236, 50237], never)).toEqual([
         50237,
      ]);
   });

   it("is empty when the named port is not ours (no retry would help)", async () => {
      const out = "error: Port 9999 in use; pass --admin_port <n>\n";
      expect(await portsTakenAtExit(out, [50236, 50237], never)).toEqual([]);
   });

   it("with nothing named, is whichever of ours is still held", async () => {
      const busy = new Set([50237]);
      const probe = async (p: number): Promise<boolean> => !busy.has(p);
      expect(await portsTakenAtExit("", [50236, 50237], probe)).toEqual([
         50237,
      ]);
      expect(
         await portsTakenAtExit(
            "PUBLISHER_INIT_FAILED",
            [1, 2],
            async () => true,
         ),
      ).toEqual([]);
   });
});

describe("waitFor with a signal", () => {
   it("stops mid-interval when the signal aborts, without another attempt", async () => {
      const ac = new AbortController();
      let calls = 0;
      const start = performance.now();
      const wait = waitFor(
         "never",
         async () => {
            calls++;
            return false;
         },
         { timeoutMs: 60_000, intervalMs: 10_000, signal: ac.signal },
      );
      setTimeout(() => ac.abort(), 50);
      await expect(wait).rejects.toThrow("stopped waiting for never");
      expect(performance.now() - start).toBeLessThan(2_000);
      expect(calls).toBe(1);
   });

   it("still resolves with the value when no signal aborts", async () => {
      let n = 0;
      const v = await waitFor("third try", async () => ++n >= 3 && n, {
         intervalMs: 1,
         signal: new AbortController().signal,
      });
      expect(v).toBe(3);
   });
});
