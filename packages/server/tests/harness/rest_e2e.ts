import http from "http";

// Force a fresh storage schema on boot so REST E2E tests never inherit
// state from a prior run's publisher.db. Set at module scope so this
// runs before any consumer dynamically imports src/server — which is
// when EnvironmentStore reads the env var at construction time.
process.env.INITIALIZE_STORAGE = "true";

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
 * Callers are responsible for creating any test-specific projects
 * via the REST API (POST /api/v0/projects) and cleaning them up.
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

   const maxWait = 180_000;
   const start = Date.now();
   let ready = false;
   while (!ready && Date.now() - start < maxWait) {
      try {
         const res = await fetch(`${baseUrl}/health/readiness`);
         if (res.ok) {
            const data = (await res.json()) as { status: string };
            if (data.status === "UP") {
               ready = true;
               break;
            }
         }
      } catch {
         // server not ready yet
      }
      await new Promise((r) => setTimeout(r, 500));
   }
   if (!ready) {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
      throw new Error("REST E2E server did not become ready in time");
   }

   const stop = async (): Promise<void> => {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
   };

   return { httpServer, baseUrl, stop };
}
