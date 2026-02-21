/**
 * Health check endpoints and graceful shutdown handling.
 *
 * Provides Kubernetes-compatible health probes:
 * - /health - Overall health status
 * - /health/liveness - Always returns 200 (process is alive)
 * - /health/readiness - Returns 200 when ready, 503 when not ready
 *
 * On SIGTERM, the service enters draining state to allow in-flight requests
 * to complete before shutdown.
 */

import { Express, NextFunction, Request, Response } from "express";
import { Server } from "http";
import { components } from "./api";
import { shutdownSDK } from "./instrumentation";
import { logger } from "./logger";
export type OperationalState =
   components["schemas"]["ServerStatus"]["operationalState"];

let operationalState: OperationalState = "initializing" as OperationalState;
let ready: boolean = false;
let preGracefulShutdownCompleted: boolean = false;
/**
 * Returns the current operational state of the service.
 */
export function getOperationalState(): OperationalState {
   return operationalState;
}

/**
 * Marks the service as ready to serve traffic.
 */
export function markReady(): void {
   if (operationalState !== "draining") {
      operationalState = "serving";
      ready = true;
      logger.info("Service marked as ready");
   } else {
      logger.error("Service is already draining - cannot mark as ready");
   }
}

/**
 * Marks the service as not ready (readiness probe will return 503).
 */
export function markNotReady(): void {
   ready = false;
   logger.info("Service marked as not ready - readiness probe will fail");
}

/**
 * Registers SIGTERM handler for graceful shutdown.
 *
 * Shutdown sequence:
 * 1. Marks service as not ready (readiness probe returns 503) and enters draining state
 * 2. Waits shutdownDrainDurationSeconds to allow in-flight requests to complete
 * 3. Sets preGracefulShutdownCompleted flag (enables drainingGuard middleware to reject new requests)
 * 4. Closes main server and MCP server (stops accepting new connections)
 * 5. Closes logger
 * 6. Waits shutdownGracefulCloseTimeoutSeconds (if > 0) for final cleanup
 * 7. Exits process
 *
 * Note: drainingGuard only rejects requests after step 3 completes. During step 2,
 * the service is draining but still accepts requests (readiness probe returns 503).
 *
 * @param server - Main HTTP server instance
 * @param mcpServer - MCP server instance
 * @param shutdownDrainDurationSeconds - Duration in seconds to wait before closing servers
 * @param shutdownGracefulCloseTimeoutSeconds - Duration in seconds to wait after closing servers before exit
 */
export function registerSignalHandlers(
   server: Server,
   mcpServer: Server,
   shutdownDrainDurationSeconds: number = 0,
   shutdownGracefulCloseTimeoutSeconds: number = 0,
): void {
   // Keep the process alive on SIGTERM — do not close the server.
   // K8s will SIGKILL after terminationGracePeriodSeconds (which cannot be caught).
   process.once("SIGTERM", async () => {
      logger.info("========== SIGTERM RECEIVED ==========");
      markNotReady();
      operationalState = "draining" as OperationalState;
      logger.info(
         `Service entering draining state for ${shutdownDrainDurationSeconds} seconds before closing servers...`,
      );

      await new Promise((resolve) =>
         setTimeout(() => {
            preGracefulShutdownCompleted = true;
            resolve(true);
         }, shutdownDrainDurationSeconds * 1000),
      );

      const closeServer = (server: Server, name: string) =>
         new Promise<void>((resolve) => {
            if (server && server.listening) {
               server.close((err) => {
                  if (err) {
                     logger.error(`${name} close error:`, err);
                  } else {
                     logger.info(`${name} closed`);
                  }
                  resolve();
               });
            } else {
               resolve();
            }
         });

      await Promise.all([
         closeServer(server, "Main server"),
         closeServer(mcpServer, "MCP server"),
      ]);

      try {
         await shutdownSDK();
         logger.info("OpenTelemetry SDK shut down");
      } catch (_error) {
         /* do nothing */
      }

      try {
         logger.close();
      } catch (_error) {
         /* do nothing */
      }

      if (shutdownGracefulCloseTimeoutSeconds > 0) {
         logger.info(
            `Waiting ${shutdownGracefulCloseTimeoutSeconds} seconds after server close before exit...`,
         );
         await new Promise((resolve) =>
            setTimeout(resolve, shutdownGracefulCloseTimeoutSeconds * 1000),
         );
      }
      process.exit(0);
   });
}
/**
 * Middleware that returns 503 for non-health and metrics requests when service is draining.
 * Must be registered before application routes.
 */
export function drainingGuard(
   req: Request,
   res: Response,
   next: NextFunction,
): void {
   if (
      operationalState === "draining" &&
      preGracefulShutdownCompleted &&
      !req.path.startsWith("/health") &&
      !req.path.startsWith("/metrics")
   ) {
      res.status(503).json({
         message: "Service is draining",
         details: "The service is shutting down and hence is not available.",
      });
      return;
   }
   next();
}

/**
 * Registers health check endpoints
 */
export function registerHealthEndpoints(app: Express): void {
   app.get("/health", (_req: Request, res: Response) => {
      res.status(200).json({
         status: "UP",
         components: {
            livenessState: { status: "UP" },
            readinessState: { status: ready ? "UP" : "DOWN" },
         },
         groups: ["livenessState", "readinessState"],
      });
   });

   app.get("/health/liveness", (_req: Request, res: Response) => {
      res.status(200).json({ status: "UP" });
   });

   app.get("/health/readiness", (_req: Request, res: Response) => {
      const isReady = ready;
      res.status(isReady ? 200 : 503).json({
         status: isReady ? "UP" : "DOWN",
      });
   });
}
