// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Ports for the publishers a hammer run starts. Every server gets ports the OS
// says are free at the moment they are handed out, so two runs on one machine
// never pick the same ones, and none is handed out twice within a run while the
// server holding it is still up.
//
// A port is free only until someone binds it, and the server binds it a moment
// later, in another process. That gap cannot be closed from here, so the caller
// retries a boot that fails with "port in use" on fresh ports
// (`PublisherCluster.boot` in run.ts, on a `PortInUseError` from startServer).

import net from "node:net";

export const HOST = "127.0.0.1";

/** Bind `port` (0 = any) on `host`, report the bound port, and keep it open. */
function bind(port: number, host: string): Promise<net.Server> {
   return new Promise((resolve, reject) => {
      const server = net.createServer();
      server.unref();
      server.once("error", reject);
      server.listen({ port, host, exclusive: true }, () => resolve(server));
   });
}

function close(server: net.Server): Promise<void> {
   return new Promise((resolve) => server.close(() => resolve()));
}

function boundPort(server: net.Server): number {
   const addr = server.address();
   if (addr === null || typeof addr === "string") {
      throw new Error("listener has no TCP address");
   }
   return addr.port;
}

/** True when `port` can be bound on `host` right now. */
export async function isPortFree(
   port: number,
   host: string = HOST,
): Promise<boolean> {
   try {
      await close(await bind(port, host));
      return true;
   } catch {
      return false;
   }
}

/**
 * Hands out ports for one run.
 *
 * With no base, each port is one the OS assigns to a bind of port 0, so it is
 * free and, being ephemeral, unlikely to be anyone's fixed choice. With a base
 * (`--port` / `--mcp-port`), ports are the free ones walking upward from it,
 * which keeps an explicit choice predictable while still stepping over a port
 * another run or process holds.
 *
 * Either way a port is never handed out again until `release`d, so the run's
 * own servers never collide with each other.
 */
export class PortAllocator {
   private readonly taken = new Set<number>();

   constructor(
      private readonly host: string = HOST,
      /** For tests: the probe that says whether a port is bindable. */
      private readonly probe: (
         port: number,
         host: string,
      ) => Promise<boolean> = isPortFree,
   ) {}

   /** Ports currently handed out (for tests and diagnostics). */
   get inUse(): ReadonlySet<number> {
      return this.taken;
   }

   /**
    * `count` distinct free ports. From the OS when `base` is unset; otherwise
    * the next free ports at or above `base`.
    */
   async allocate(count: number, base?: number): Promise<number[]> {
      return base === undefined
         ? this.fromOs(count)
         : this.fromBase(count, base);
   }

   /** Return ports whose server has stopped, so they may be handed out again. */
   release(ports: Iterable<number>): void {
      for (const p of ports) this.taken.delete(p);
   }

   private async fromOs(count: number): Promise<number[]> {
      // Every listener stays open until all are bound, so the OS cannot hand the
      // same port to two of them. One it gives back that this run already handed
      // out (its server has not bound it yet, or the OS recycled it) is skipped.
      const held: net.Server[] = [];
      const ports: number[] = [];
      try {
         for (let tries = 0; ports.length < count; tries++) {
            if (tries > count + 64) {
               throw new Error(
                  `could not get ${count} free port(s) from the OS on ${this.host}`,
               );
            }
            const server = await bind(0, this.host);
            held.push(server);
            const port = boundPort(server);
            if (!this.taken.has(port) && !ports.includes(port))
               ports.push(port);
         }
      } finally {
         await Promise.all(held.map(close));
      }
      for (const p of ports) this.taken.add(p);
      return ports;
   }

   private async fromBase(count: number, base: number): Promise<number[]> {
      if (!Number.isInteger(base) || base < 1 || base > 65535) {
         throw new Error(`invalid base port ${base}`);
      }
      const ports: number[] = [];
      let p = base;
      while (ports.length < count) {
         if (p > 65535) {
            throw new Error(`no free port at or above ${base} on ${this.host}`);
         }
         if (!this.taken.has(p) && (await this.probe(p, this.host))) {
            ports.push(p);
            this.taken.add(p);
         }
         p++;
      }
      return ports;
   }
}

/** A server exited because a port it was given was taken in the meantime. */
export class PortInUseError extends Error {
   constructor(
      message: string,
      readonly ports: number[],
   ) {
      super(message);
      this.name = "PortInUseError";
   }
}

/** Publisher's own wording for a failed bind, or the errno behind it. */
export function looksLikePortInUse(output: string): boolean {
   return /EADDRINUSE|Port \d+ in use|address already in use/i.test(output);
}

/** The ports Publisher said were in use (`Port N in use; pass --port <n>`). */
export function portsReportedInUse(output: string): number[] {
   return [...output.matchAll(/Port (\d+) in use/g)].map((m) => Number(m[1]));
}
