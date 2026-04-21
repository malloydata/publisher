import http from "http";

export interface RestE2EEnv {
   httpServer: http.Server;
   baseUrl: string;
}

// Synchronous stderr logger — bun's stdout buffering can swallow
// `console.log` output written from a failed `beforeAll`, making CI
// failures impossible to diagnose without a local repro. stderr flushes
// line-by-line and survives across suite boundaries.
function diag(msg: string): void {
   process.stderr.write(`[REST E2E] ${msg}\n`);
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
   diag("importing server module...");
   const { app } = await import("../../src/server");
   diag("server module imported");

   const httpServer: http.Server = await new Promise<http.Server>(
      (resolve, reject) => {
         const srv = http
            .createServer(app)
            .listen(0, "127.0.0.1", () => resolve(srv));
         srv.on("error", (err: NodeJS.ErrnoException) => {
            diag(
               `server listen error: ${err.code ?? ""} ${err.message ?? err}`,
            );
            reject(err);
         });
      },
   );

   const addr = httpServer.address() as { port: number };
   const baseUrl = `http://127.0.0.1:${addr.port}`;
   diag(`listening on ${baseUrl}, polling readiness...`);

   // Keep the readiness wait below bun's default 100s test timeout so a
   // stuck startup fails with a useful error rather than the whole
   // beforeAll timing out silently (which produces 1ms fake-failures for
   // every `it` in the suite — hard to diagnose on CI).
   const maxWait = 90_000;
   const start = Date.now();
   let ready = false;
   let lastStatus: string | undefined;
   let lastError: unknown;
   while (!ready && Date.now() - start < maxWait) {
      try {
         const res = await fetch(`${baseUrl}/health/readiness`);
         if (res.ok) {
            const data = (await res.json()) as { status: string };
            lastStatus = data.status;
            if (data.status === "UP") {
               ready = true;
               break;
            }
         } else {
            lastStatus = `HTTP ${res.status}`;
         }
      } catch (err) {
         lastError = err;
      }
      await new Promise((r) => setTimeout(r, 500));
   }
   if (!ready) {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
      const detail = lastStatus
         ? `last status: ${lastStatus}`
         : `last error: ${lastError instanceof Error ? lastError.message : String(lastError)}`;
      const msg = `REST E2E server did not become ready within ${maxWait / 1000}s (${detail})`;
      diag(msg);
      throw new Error(msg);
   }
   diag(`ready (took ${Date.now() - start}ms)`);

   const stop = async (): Promise<void> => {
      httpServer.closeAllConnections?.();
      await new Promise<void>((r) => httpServer.close(() => r()));
   };

   return { httpServer, baseUrl, stop };
}
