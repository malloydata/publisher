import * as fs from "fs";
import * as path from "path";
import { logger } from "../logger";
import { Environment } from "./environment";
import type { EnvironmentStore } from "./environment_store";

/**
 * Marker suffix for the parent directory that holds versioned package
 * checkouts: `<env>/<pkg>.versions/<uuid>`. The sweeper only ever touches
 * directories inside a `*.versions/` folder so externally-mounted package
 * paths (absolute paths from `publisher.config.json`) are not at risk.
 */
export const VERSIONED_PACKAGE_DIR_SUFFIX = ".versions";

/**
 * Default time a versioned package directory must be unreferenced before the
 * sweeper deletes it. Sized comfortably larger than any expected query so
 * an in-flight reader holding a cached `Package` against the old directory
 * always finishes before the directory disappears.
 */
const DEFAULT_QUIESCENCE_MS = 30 * 60 * 1000;
/** Default cadence for the periodic full-environment sweep. */
const DEFAULT_PERIODIC_SWEEP_INTERVAL_MS = 15 * 60 * 1000;

/**
 * Reclaims orphaned versioned package directories produced by the
 * UUID-staging + DB-CAS write path in `PackageController`.
 *
 * Two entry points:
 *
 *   - `scheduleSweep(env, dir)` — deferred per-path cleanup the controller
 *     calls after a successful swap. The directory is `rm -rf`'d after the
 *     quiescence window so any in-flight reader that picked the path up
 *     before the swap has finished its query.
 *
 *   - `sweepEnvironment` / periodic `start()` — backstop that catches
 *     orphans from process crashes (download committed to disk but the
 *     `swapPackageDirectory` row never landed, or the inverse).
 *
 * Both paths drop the corresponding entry from the environment's path-keyed
 * package cache and retire the package's MalloyConfig connections through
 * the existing `retireConnectionGeneration` machinery so connection cleanup
 * follows directory cleanup automatically.
 */
export class PackageSweeper {
   private readonly quiescenceMs: number;
   private readonly periodicIntervalMs: number;
   private readonly scheduledTimers = new Map<string, NodeJS.Timeout>();
   private periodicTimer?: NodeJS.Timeout;
   private stopped = false;

   constructor(
      private store: EnvironmentStore,
      options?: { quiescenceMs?: number; periodicIntervalMs?: number },
   ) {
      this.quiescenceMs = options?.quiescenceMs ?? DEFAULT_QUIESCENCE_MS;
      this.periodicIntervalMs =
         options?.periodicIntervalMs ?? DEFAULT_PERIODIC_SWEEP_INTERVAL_MS;
   }

   /**
    * Start the periodic full sweep. Idempotent — a second call is a no-op
    * so callers do not have to track lifecycle separately. The very first
    * sweep also runs immediately so orphans from the previous process exit
    * are reclaimed at startup, not the next interval boundary.
    */
   public start(): void {
      if (this.stopped) return;
      if (this.periodicTimer) return;

      void this.sweepAllEnvironments().catch((error) => {
         logger.warn("Initial package sweep failed", { error });
      });

      this.periodicTimer = setInterval(() => {
         void this.sweepAllEnvironments().catch((error) => {
            logger.warn("Periodic package sweep failed", { error });
         });
      }, this.periodicIntervalMs);
      this.periodicTimer.unref?.();
   }

   /**
    * Stop the periodic sweep and cancel any pending scheduled cleanups so
    * the process can exit cleanly.
    */
   public stop(): void {
      this.stopped = true;
      if (this.periodicTimer) {
         clearInterval(this.periodicTimer);
         this.periodicTimer = undefined;
      }
      for (const timer of this.scheduledTimers.values()) {
         clearTimeout(timer);
      }
      this.scheduledTimers.clear();
   }

   /**
    * Schedule a versioned package directory for cleanup after the quiescence
    * window. Idempotent on `(envName, dirPath)`; repeated calls coalesce.
    *
    * Skips paths that fall outside an `*.versions/` directory, which is how
    * we avoid sweeping externally-mounted package paths declared in
    * `publisher.config.json`.
    */
   public scheduleSweep(envName: string, dirPath: string): void {
      if (this.stopped) return;
      if (!dirPath || !this.isVersionedPackagePath(dirPath)) return;

      const key = `${envName}|${dirPath}`;
      if (this.scheduledTimers.has(key)) return;

      const timer = setTimeout(() => {
         this.scheduledTimers.delete(key);
         void this.sweepPath(envName, dirPath).catch((error) => {
            logger.warn("Scheduled package sweep failed", {
               envName,
               dirPath,
               error,
            });
         });
      }, this.quiescenceMs);
      timer.unref?.();
      this.scheduledTimers.set(key, timer);
   }

   /**
    * Visible for tests / startup hooks: run a single full sweep across every
    * environment immediately. Reads each environment's referenced directory
    * set from the database and reclaims everything else under
    * `<envPath>/*.versions/*` that is older than the quiescence window.
    */
   public async sweepAllEnvironments(): Promise<void> {
      const environments = this.store.listInMemoryEnvironments();
      for (const environment of environments) {
         try {
            await this.sweepEnvironment(environment);
         } catch (error) {
            logger.warn("Failed to sweep environment", {
               envName: environment.getEnvironmentName(),
               error,
            });
         }
      }
   }

   private async sweepEnvironment(environment: Environment): Promise<void> {
      const envName = environment.getEnvironmentName();
      const repository = this.store.storageManager.getRepository();
      const dbEnv = await repository.getEnvironmentByName(envName);
      if (!dbEnv) return;

      const referenced =
         await repository.listPackageDirectoryPaths(dbEnv.id);
      const versionsRoots = await this.findVersionsRoots(
         environment.getEnvironmentPath(),
      );
      const now = Date.now();

      for (const versionsRoot of versionsRoots) {
         let entries: fs.Dirent[];
         try {
            entries = await fs.promises.readdir(versionsRoot, {
               withFileTypes: true,
            });
         } catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            if (code === "ENOENT") continue;
            throw error;
         }

         for (const entry of entries) {
            if (!entry.isDirectory()) continue;
            const dirPath = path.join(versionsRoot, entry.name);
            if (referenced.has(dirPath)) continue;

            try {
               const stat = await fs.promises.stat(dirPath);
               if (now - stat.mtimeMs < this.quiescenceMs) continue;
            } catch {
               continue;
            }

            await this.removeOrphan(environment, dirPath);
         }
      }
   }

   /**
    * Removes one specific directory if (a) it falls under a versions root,
    * (b) it is no longer referenced by the database, and (c) it has aged
    * past the quiescence window. The triple-check makes scheduled sweeps
    * safe to call even when something else races to swap the package back
    * to the same path between the schedule and the timer firing.
    */
   private async sweepPath(envName: string, dirPath: string): Promise<void> {
      if (!this.isVersionedPackagePath(dirPath)) return;

      const environment = await this.store
         .getEnvironment(envName, false)
         .catch(() => undefined);
      if (!environment) {
         await this.bestEffortRm(dirPath);
         return;
      }

      const repository = this.store.storageManager.getRepository();
      const dbEnv = await repository.getEnvironmentByName(envName);
      if (dbEnv) {
         const referenced =
            await repository.listPackageDirectoryPaths(dbEnv.id);
         if (referenced.has(dirPath)) return;
      }

      try {
         const stat = await fs.promises.stat(dirPath);
         if (Date.now() - stat.mtimeMs < this.quiescenceMs) {
            // Race: someone touched the directory after we scheduled it.
            // Treat it like a fresh write and let the next periodic sweep
            // re-check.
            return;
         }
      } catch {
         return;
      }

      await this.removeOrphan(environment, dirPath);
   }

   private async removeOrphan(
      environment: Environment,
      dirPath: string,
   ): Promise<void> {
      environment.evictPackageAtPath(dirPath);
      await this.bestEffortRm(dirPath);
      logger.info("Reclaimed orphaned package directory", {
         envName: environment.getEnvironmentName(),
         dirPath,
      });
   }

   private async bestEffortRm(dirPath: string): Promise<void> {
      try {
         await fs.promises.rm(dirPath, { recursive: true, force: true });
      } catch (error) {
         logger.warn("Failed to remove orphaned package directory", {
            dirPath,
            error,
         });
      }
   }

   private isVersionedPackagePath(dirPath: string): boolean {
      const parent = path.basename(path.dirname(dirPath));
      return parent.endsWith(VERSIONED_PACKAGE_DIR_SUFFIX);
   }

   private async findVersionsRoots(envPath: string): Promise<string[]> {
      let entries: fs.Dirent[];
      try {
         entries = await fs.promises.readdir(envPath, { withFileTypes: true });
      } catch (error) {
         const code = (error as NodeJS.ErrnoException).code;
         if (code === "ENOENT") return [];
         throw error;
      }
      return entries
         .filter(
            (entry) =>
               entry.isDirectory() &&
               entry.name.endsWith(VERSIONED_PACKAGE_DIR_SUFFIX),
         )
         .map((entry) => path.join(envPath, entry.name));
   }
}

/**
 * Helper used by `PackageController` to construct a fresh, unique versioned
 * directory path for an upcoming download. Using `<pkgName>.versions/<uuid>`
 * keeps every version of a single package co-located on disk so the sweeper
 * can reclaim them with a single readdir per package.
 */
export function buildVersionedPackagePath(
   environmentPath: string,
   packageName: string,
   versionId: string,
): string {
   return path.join(
      environmentPath,
      `${packageName}${VERSIONED_PACKAGE_DIR_SUFFIX}`,
      versionId,
   );
}
