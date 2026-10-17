// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import express from "express";
import { Server } from "http";
import request from "supertest";
import { logger } from "./logger";
import { __setPackageLoadPoolForTests } from "./package_load/package_load_pool";

type Health = typeof import("./health");

// health.ts keeps its state at module level and has no reset, so each test
// imports its own instance (the query string makes a separate module record).
// Draining is one-way; a shared instance would leave every later spec in this
// process unable to mark the server ready.
let instance = 0;
async function freshHealth(): Promise<Health> {
   instance += 1;
   return (await import(`./health?instance=${instance}`)) as Health;
}

/** A bare app with the guard in front, the probes, and two ordinary routes. */
function appFor(health: Health) {
   const app = express();
   app.use(health.drainingGuard);
   health.registerHealthEndpoints(app);
   app.get("/api/v0/status", (_req, res) => res.json({ ok: true }));
   app.get("/metrics", (_req, res) => res.send("metrics"));
   return app;
}

async function probes(app: express.Express) {
   const [health, liveness, readiness] = await Promise.all([
      request(app).get("/health"),
      request(app).get("/health/liveness"),
      request(app).get("/health/readiness"),
   ]);
   return {
      health: [health.status, health.body.components?.readinessState?.status],
      liveness: [liveness.status, liveness.body.status],
      readiness: [readiness.status, readiness.body.status],
   };
}

/** A listening server whose close() is recorded. */
function fakeServer(name: string, closed: string[]): Server {
   return {
      listening: true,
      close: (cb: (err?: Error) => void) => {
         closed.push(name);
         cb();
      },
   } as unknown as Server;
}

let calls: string[];
const originalExit = process.exit;

beforeEach(() => {
   calls = [];
   spyOn(logger, "info").mockImplementation(((msg: string) => {
      calls.push(`info:${msg}`);
      return logger;
   }) as never);
   spyOn(logger, "warn").mockImplementation((() => logger) as never);
   spyOn(logger, "error").mockImplementation((() => logger) as never);
   // Closing the real logger would silence every later spec in this process.
   spyOn(logger, "close").mockImplementation((() => {
      calls.push("close");
      return logger;
   }) as never);
   process.exit = ((code?: number) => {
      calls.push(`exit:${code}`);
   }) as never;
});

afterEach(() => {
   process.exit = originalExit;
   mock.restore();
});

// Shutdown drains the package-load pool through getPackageLoadPool(), which
// lazily creates the global singleton and leaves it shut down. Later specs in
// this process would inherit a dead pool; reset it so they get a fresh one.
afterAll(async () => {
   await __setPackageLoadPoolForTests(null);
});

describe("operational state and probes", () => {
   it("starts initializing: alive but not ready", async () => {
      const health = await freshHealth();
      expect(health.getOperationalState()).toBe("initializing");
      expect(await probes(appFor(health))).toEqual({
         health: [200, "DOWN"],
         liveness: [200, "UP"],
         readiness: [503, "DOWN"],
      });
   });

   it("markReady moves to serving and the readiness probe passes", async () => {
      const health = await freshHealth();
      health.markReady();
      expect(health.getOperationalState()).toBe("serving");
      expect(await probes(appFor(health))).toEqual({
         health: [200, "UP"],
         liveness: [200, "UP"],
         readiness: [200, "UP"],
      });
   });

   it("markNotReady fails readiness without leaving serving", async () => {
      const health = await freshHealth();
      health.markReady();
      health.markNotReady();
      expect(health.getOperationalState()).toBe("serving");
      expect(await probes(appFor(health))).toEqual({
         health: [200, "DOWN"],
         liveness: [200, "UP"],
         readiness: [503, "DOWN"],
      });
      health.markReady();
      expect(
         (await request(appFor(health)).get("/health/readiness")).status,
      ).toBe(200);
   });

   it("lets every request through while not draining", async () => {
      const health = await freshHealth();
      const app = appFor(health);
      expect((await request(app).get("/api/v0/status")).status).toBe(200);
      health.markReady();
      expect((await request(app).get("/api/v0/status")).status).toBe(200);
   });
});

describe("SIGTERM: drain, then refuse, then shut down", () => {
   it("fails readiness at once, refuses app routes only after the drain, and closes both servers", async () => {
      const health = await freshHealth();
      health.markReady();
      const app = appFor(health);

      // Capture the handler rather than registering it, so the test can run
      // it without sending this process a real signal.
      let onSigterm: (() => Promise<void>) | undefined;
      spyOn(process, "once").mockImplementation(((
         event: string,
         handler: () => Promise<void>,
      ) => {
         if (event === "SIGTERM") onSigterm = handler;
         return process;
      }) as never);
      const closed: string[] = [];
      health.registerSignalHandlers(
         fakeServer("main", closed),
         fakeServer("mcp", closed),
         0.05,
         0,
      );
      expect(onSigterm).toBeDefined();

      const shutdown = onSigterm!();

      // Observed during the drain window, asserted once shutdown has settled
      // so a failure here cannot leave it running into the next test.
      const stateWhileDraining = health.getOperationalState();
      const probesWhileDraining = await probes(app);
      const appRouteWhileDraining = (await request(app).get("/api/v0/status"))
         .status;
      health.markReady(); // a late markReady must not undo draining
      const stateAfterLateReady = health.getOperationalState();
      const readinessAfterLateReady = (
         await request(app).get("/health/readiness")
      ).status;

      await shutdown;

      // Draining window: readiness fails, liveness holds, requests still served.
      expect(stateWhileDraining).toBe("draining");
      expect(probesWhileDraining).toEqual({
         health: [200, "DOWN"],
         liveness: [200, "UP"],
         readiness: [503, "DOWN"],
      });
      expect(appRouteWhileDraining).toBe(200);
      expect(stateAfterLateReady).toBe("draining");
      expect(readinessAfterLateReady).toBe(503);

      // After the drain the guard refuses app routes but not probes or metrics.
      const refused = await request(app).get("/api/v0/status");
      expect(refused.status).toBe(503);
      expect(refused.body.message).toBe("Service is draining");
      expect((await request(app).get("/health/liveness")).status).toBe(200);
      expect((await request(app).get("/metrics")).status).toBe(200);
      expect(closed.sort()).toEqual(["main", "mcp"]);
      expect(calls.at(-1)).toBe("exit:0");
   });
});

// Regression: winston printed "Attempt to write logs with no transports" for
// the "Waiting N seconds..." line because the logger was closed before it.
describe("performGracefulShutdownAfterDrain", () => {
   const idle = () => ({ listening: false }) as unknown as Server;

   it("logs the graceful-close wait, then closes the logger once, then exits", async () => {
      const health = await freshHealth();
      await health.performGracefulShutdownAfterDrain(idle(), idle(), 0.05);

      const waiting = calls.findIndex((c) => c.startsWith("info:Waiting"));
      const close = calls.indexOf("close");
      expect(waiting).toBeGreaterThanOrEqual(0);
      expect(waiting).toBeLessThan(close);
      expect(calls.filter((c) => c === "close")).toHaveLength(1);
      expect(calls.slice(close + 1)).toEqual(["exit:0"]);
   });

   it("skips the wait when the graceful-close timeout is 0", async () => {
      const health = await freshHealth();
      await health.performGracefulShutdownAfterDrain(idle(), idle(), 0);

      expect(calls.some((c) => c.startsWith("info:Waiting"))).toBe(false);
      expect(calls.slice(-2)).toEqual(["close", "exit:0"]);
   });
});
