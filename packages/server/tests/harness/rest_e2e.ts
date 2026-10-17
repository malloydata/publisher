// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import http from "http";
import { pollUntil } from "./spawn_server";

export interface RestE2EEnv {
   httpServer: http.Server;
   baseUrl: string;
}

/**
 * Spin up an HTTP server wrapping the real Express REST app.
 *
 * Works regardless of which test file first imported server.ts —
 * reuses the cached Express app and binds on an OS-assigned port
 * to avoid collisions.
 *
 * Callers are responsible for creating any test-specific environments
 * via the REST API (POST /api/v0/environments) and cleaning them up.
 */
export async function startRestE2E(): Promise<
   RestE2EEnv & { stop(): Promise<void> }
> {
   const { app } = await import("../../src/server");

   const httpServer: http.Server = await new Promise<http.Server>(
      (resolve, reject) => {
         const srv = http
            .createServer(app)
            .listen(0, "127.0.0.1", () => resolve(srv));
         srv.on("error", (err: NodeJS.ErrnoException) => {
            console.error("[REST E2E] server listen error", err);
            reject(err);
         });
      },
   );

   const addr = httpServer.address() as { port: number };
   const baseUrl = `http://127.0.0.1:${addr.port}`;

   const stop = async (): Promise<void> => {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
   };

   const ready = await pollUntil(async () => {
      try {
         const res = await fetch(`${baseUrl}/health/readiness`);
         if (!res.ok) return false;
         return ((await res.json()) as { status: string }).status === "UP";
      } catch {
         return false;
      }
   }, 180_000);
   if (!ready) {
      await stop();
      throw new Error("REST E2E server did not become ready in time");
   }

   return { httpServer, baseUrl, stop };
}
