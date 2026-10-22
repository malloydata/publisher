// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The socket limits as server.ts applies them, on a real server with a short
 * limit.
 *
 * Bun 1.4 ends a request `server.timeout` after it started, even one that is
 * still writing; Node waits for the socket to go quiet. So the limit has to
 * outlast the slowest answer (`getServerSocketTimeoutMs` derives it from the
 * query timeout), and the `/events` stream, which stays open by design, has to
 * opt out. Without the opt-out the stream test fails on Bun 1.4.3: the server
 * closes the stream at the limit.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { EventEmitter } from "events";
import express from "express";
import http from "http";
import net, { type AddressInfo } from "net";

import { applyServerTimeouts } from "./server_timeouts";
import { API_PREFIX, buildRouteApp } from "./test_helpers/route_app";

const servers: http.Server[] = [];

afterEach(() => {
   for (const server of servers.splice(0)) {
      server.closeAllConnections();
      server.close();
   }
});

async function listen(
   app: express.Express,
   socketTimeoutMs: number,
): Promise<number> {
   const server = http.createServer(app);
   applyServerTimeouts(server, socketTimeoutMs);
   servers.push(server);
   await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
   return (server.address() as AddressInfo).port;
}

/**
 * GETs `path` on a raw socket and waits up to `holdMs` for the server to close
 * it. Returns what arrived and whether the server closed the socket.
 */
async function get(
   port: number,
   path: string,
   holdMs: number,
): Promise<{ data: string; closedByServer: boolean }> {
   const socket = net.connect(port, "127.0.0.1");
   socket.on("error", () => undefined);
   await new Promise((r) => socket.once("connect", r));
   let data = "";
   let closed = false;
   socket.on("data", (chunk) => (data += chunk.toString()));
   socket.once("close", () => (closed = true));
   socket.write(`GET ${path} HTTP/1.1\r\nHost: localhost\r\n\r\n`);
   const deadline = Date.now() + holdMs;
   while (!closed && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
   }
   const closedByServer = closed;
   socket.destroy();
   return { data, closedByServer };
}

const SLOW_HANDLER_MS = 600;

/** Answers `/slow` after {@link SLOW_HANDLER_MS}, sending nothing before then. */
function slowApp(): express.Express {
   const app = express();
   app.get("/slow", (_req, res) => {
      setTimeout(() => {
         if (!res.writableEnded) res.status(200).send("done");
      }, SLOW_HANDLER_MS);
   });
   return app;
}

describe("applyServerTimeouts", () => {
   it("cuts a request that outlives the socket timeout", async () => {
      const port = await listen(slowApp(), SLOW_HANDLER_MS / 2);
      const { data, closedByServer } = await get(port, "/slow", 1500);
      expect(closedByServer).toBe(true);
      expect(data).not.toContain("200");
   });

   it("answers the same request once the timeout is raised past it", async () => {
      const port = await listen(slowApp(), SLOW_HANDLER_MS * 2);
      const { data } = await get(port, "/slow", 1500);
      expect(data).toStartWith("HTTP/1.1 200");
   });

   it("keeps the /events stream open past the socket timeout", async () => {
      const port = await listen(
         buildRouteApp({
            environmentStore: {
               getEnvironment: async () =>
                  ({ getPackage: async () => ({}) }) as never,
            },
            watchModeController: {
               // Registered as route handlers; this test never calls them.
               getWatchStatus: () => undefined,
               startWatching: () => undefined,
               stopWatchMode: () => undefined,
               isWatching: () => false,
               events: new EventEmitter(),
            } as never,
         }),
         300,
      );
      const { data, closedByServer } = await get(
         port,
         `${API_PREFIX}/environments/env/packages/pkg/events`,
         1200,
      );
      expect(data).toContain("event: hello");
      expect(closedByServer).toBe(false);
   });
});
