// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Kubernetes health probes (`/health`, `/health/liveness`, `/health/readiness`)
 * and SIGTERM draining.
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

export function getOperationalState(): OperationalState {
   return operationalState;
}

export function markReady(): void {
   if (operationalState !== "draining") {
      operationalState = "serving";
      ready = true;
      logger.info("Service marked as ready");
   } else {
      logger.error("Service is already draining - cannot mark as ready");
   }
}

export function markNotReady(): void {
   ready = false;
   logger.info("Service marked as not ready - readiness probe will fail");
}

/**
 * On SIGTERM: fail readiness and drain for `shutdownDrainDurationSeconds`
 * while still accepting requests, then let `drainingGuard` refuse new ones,
 * close both servers, wait `shutdownGracefulCloseTimeoutSeconds`, close the
 * logger and exit.
 */
export function registerSignalHandlers(
   server: Server,
   mcpServer: Server,
   shutdownDrainDurationSeconds: number = 0,
   shutdownGracefulCloseTimeoutSeconds: number = 0,
): void {
   // K8s sends SIGKILL after terminationGracePeriodSeconds if this has not exited.
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

      await performGracefulShutdownAfterDrain(
         server,
         mcpServer,
         shutdownGracefulCloseTimeoutSeconds,
      );
   });
}

/**
 * The post-drain shutdown: close both servers, the OTel SDK and the worker
 * pool, wait the graceful-close window, close the logger, exit. Exported so
 * tests can check the ordering without a SIGTERM, which would leave
 * module-level state "draining" for sibling specs.
 */
export async function performGracefulShutdownAfterDrain(
   server: Server,
   mcpServer: Server,
   shutdownGracefulCloseTimeoutSeconds: number,
): Promise<void> {
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
      // So a slow compile leaves no orphan workers. Lazy-imported to keep the
      // pool out of this module's import graph in tests.
      const { getPackageLoadPool } = await import(
         "./package_load/package_load_pool"
      );
      await getPackageLoadPool().shutdown();
      logger.info("Package-load worker pool shut down");
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

   // Last, so logs from the wait window still reach a transport; a later
   // logger call would warn "Attempt to write logs with no transports".
   try {
      logger.close();
   } catch (_error) {
      /* do nothing */
   }

   process.exit(0);
}
/**
 * 503 for every request but health and metrics once draining has finished.
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
