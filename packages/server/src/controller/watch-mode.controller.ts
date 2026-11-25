// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { recordPackageReload } from "../package_state_metrics";
import chokidar, { FSWatcher } from "chokidar";
import { EventEmitter } from "events";
import path from "path";
import { components } from "../api";
import { EnvironmentNotFoundError } from "../errors";
import { logger } from "../logger";
import { assertSafePackageName } from "../path_safety";
import type { PackageLookup } from "../service/environment_ports";
import { EnvironmentStore } from "../service/environment_store";
import { parseEnvironmentName, parsePackageName } from "../service/identity";

type WatchStatusRes = components["schemas"]["WatchStatus"];

// Package assets: they trigger a live-reload event but no recompile.
const ASSET_EXTS = new Set([
   ".html",
   ".htm",
   ".css",
   ".js",
   ".mjs",
   ".json",
   ".png",
   ".jpg",
   ".jpeg",
   ".gif",
   ".svg",
   ".webp",
   ".ico",
   ".woff",
   ".woff2",
]);
const MODEL_EXTS = new Set([".malloy", ".malloynb", ".md"]);

/**
 * The package a watched file belongs to: the first folder under the
 * environment directory. None for a file at the environment root or outside
 * it, and none under a folder whose name starts with `.`. Those are the
 * server's own (`.staging`, `.retired`, `.legacy`, `.install-records`,
 * `.storage-destinations`), which an install writes and removes as it runs,
 * or an editor's or VCS's: never a package, since no package name starts
 * with `.` (assertSafePackageName).
 */
export function watchedPackageOf(
   watchPath: string,
   filePath: string,
): string | null {
   const segments = path.relative(watchPath, filePath).split(path.sep);
   const [first] = segments;
   if (segments.length < 2 || !first || first.startsWith(".")) return null;
   return first;
}

/**
 * Whether the watcher skips a path: anything at the environment root whose
 * name starts with `.`, so nothing under the server's own folders is watched
 * (see watchedPackageOf), and any file that is neither a model nor an asset.
 */
export function ignoredByWatch(
   watchPath: string,
   filePath: string,
   stats?: { isFile(): boolean },
): boolean {
   if (
      path.relative(watchPath, path.dirname(filePath)) === "" &&
      path.basename(filePath).startsWith(".")
   ) {
      return true;
   }
   if (!stats?.isFile()) return false;
   const ext = path.extname(filePath).toLowerCase();
   return !MODEL_EXTS.has(ext) && !ASSET_EXTS.has(ext);
}

export class WatchModeController {
   watchingPath: string | null;
   watchingEnvironmentName: string | null;
   watcher: FSWatcher | null = null;
   // Serializes ensureWatching: concurrent callers that both close and
   // recreate the watcher would orphan one and leak its OS file handles.
   private setupChain: Promise<void> | null = null;
   /**
    * Per-package change bus, event name `<environmentName>/<packageName>`,
    * feeding the SSE live-reload endpoint. Emits `{ path, kind }`.
    */
   public events = new EventEmitter();

   constructor(
      private environmentStore: PackageLookup & {
         readonly serverRootPath: string;
      },
   ) {
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
      // One live-reload listener per open browser tab.
      this.events.setMaxListeners(100);
   }

   /**
    * Idempotent: starts watching `environmentName` if not already.
    *
    * Throws if the environment can't be resolved; never falls back to a
    * `<serverRoot>/<envName>` path, which URL-encoded traversal could
    * control. Callers must validate the env name first.
    */
   public async ensureWatching(environmentName: string): Promise<void> {
      if (this.watchingEnvironmentName === environmentName && this.watcher) {
         return;
      }
      // Re-check the guard once any in-flight setup has finished.
      const run = (this.setupChain ?? Promise.resolve())
         .catch(() => {})
         .then(async () => {
            if (
               this.watchingEnvironmentName === environmentName &&
               this.watcher
            ) {
               return;
            }
            const env = await this.environmentStore.getEnvironment(
               parseEnvironmentName(environmentName),
               false,
            );
            const watchPath = env.getEnvironmentPath();
            if (this.watcher) {
               await this.watcher.close();
               this.watcher = null;
            }
            this.startWatcher(environmentName, watchPath);
         });
      this.setupChain = run;
      await run;
   }

   /** Whether watch mode is currently running for the given env. */
   public isWatching(environmentName: string): boolean {
      return !!this.watcher && this.watchingEnvironmentName === environmentName;
   }

   private startWatcher(watchName: string, watchPath: string) {
      this.watchingEnvironmentName = watchName;
      this.watchingPath = watchPath;
      this.watcher = chokidar.watch(this.watchingPath, {
         ignored: (filePath, stats) =>
            ignoredByWatch(watchPath, filePath, stats),
         ignoreInitial: true,
      });
      // Recompile just the changed file's package. An env-level reload would
      // not do: Environment.update() refreshes metadata and connections but
      // never recompiles packages. Returns false on a compile error (e.g. a
      // half-typed model saved mid-edit).
      const reloadPackage = async (pkgName: string): Promise<boolean> => {
         try {
            const environment = await this.environmentStore.getEnvironment(
               parseEnvironmentName(watchName),
               false,
            );
            await environment.getPackage(parsePackageName(pkgName), true);
            recordPackageReload("watch", "success");
            logger.info(
               `Watch: recompiled package "${pkgName}" in environment "${watchName}"`,
            );
            return true;
         } catch (error) {
            recordPackageReload("watch", "failure");
            logger.error(
               `Watch: failed to recompile package "${pkgName}" in environment "${watchName}"`,
               { error },
            );
            return false;
         }
      };
      const onEvent =
         (kind: "add" | "change" | "unlink") => async (filePath: string) => {
            logger.info(`Watch ${kind}: ${filePath}; environment=${watchName}`);
            // Packages are env/<pkg>/...; a file at the env root, or under
            // one of the server's own dot-directories, is no package's.
            const pkgName = watchedPackageOf(watchPath, filePath);
            if (!pkgName) return;

            // Assets need only the live-reload fanout. A model edit signals
            // only once it recompiles, so a transient syntax error doesn't
            // bounce open pages into a compile error or 404.
            const ext = path.extname(filePath).toLowerCase();
            if (MODEL_EXTS.has(ext)) {
               const recompiled = await reloadPackage(pkgName);
               if (!recompiled) return;
            }
            this.events.emit(`${watchName}/${pkgName}`, {
               path: filePath,
               kind,
            });
         };
      this.watcher.on("add", onEvent("add"));
      this.watcher.on("change", onEvent("change"));
      this.watcher.on("unlink", onEvent("unlink"));
   }

   /** What is being watched: the admin API's `watch-mode` resource. */
   public current(): WatchStatusRes {
      return {
         enabled: !!this.watchingPath,
         watchingPath: this.watchingPath ?? "",
         environmentName: this.watchingEnvironmentName ?? "",
      };
   }

   /**
    * Watches `environmentName`, replacing any other watch. Throws a
    * BadRequestError for an unsafe name and an EnvironmentNotFoundError for an
    * environment that is not configured or has no packages.
    */
   public async watch(environmentName: string): Promise<WatchStatusRes> {
      assertSafePackageName(environmentName);
      const environmentManifest =
         await EnvironmentStore.reloadEnvironmentManifest(
            this.environmentStore.serverRootPath,
         );
      const environment = environmentManifest.environments.find(
         (e) => e.name === environmentName,
      );
      if (
         !environment ||
         !environment.packages ||
         environment.packages.length === 0
      ) {
         throw new EnvironmentNotFoundError(
            `Environment ${environmentName} not found or has no packages`,
         );
      }
      await this.ensureWatching(environmentName);
      return this.current();
   }

   /** Stops watching. Idempotent. */
   public async unwatch(): Promise<void> {
      if (this.watcher) {
         await this.watcher.close();
         this.watcher = null;
      }
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
   }
}
