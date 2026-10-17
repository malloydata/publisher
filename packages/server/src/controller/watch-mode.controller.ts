// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { recordPackageReload } from "../package_state_metrics";
import chokidar, { FSWatcher } from "chokidar";
import { EventEmitter } from "events";
import { RequestHandler } from "express";
import path from "path";
import { components } from "../api";
import { internalErrorToHttpError } from "../errors";
import { logger } from "../logger";
import { assertSafePackageName } from "../path_safety";
import { EnvironmentStore } from "../service/environment_store";

type StartWatchReq = components["schemas"]["StartWatchRequest"];
type WatchStatusRes = components["schemas"]["WatchStatus"];
type Handler<Req = object, Res = void> = RequestHandler<object, Res, Req>;

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

   constructor(private environmentStore: EnvironmentStore) {
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
               environmentName,
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
         ignored: (filePath, stats) => {
            if (!stats?.isFile()) return false;
            const ext = path.extname(filePath).toLowerCase();
            return !MODEL_EXTS.has(ext) && !ASSET_EXTS.has(ext);
         },
         ignoreInitial: true,
      });
      // Recompile just the changed file's package. An env-level reload would
      // not do: Environment.update() refreshes metadata and connections but
      // never recompiles packages. Returns false on a compile error (e.g. a
      // half-typed model saved mid-edit).
      const reloadPackage = async (pkgName: string): Promise<boolean> => {
         try {
            const environment = await this.environmentStore.getEnvironment(
               watchName,
               false,
            );
            await environment.getPackage(pkgName, true);
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
            // Packages are env/<pkg>/...; files at the env root are ignored.
            const rel = path.relative(this.watchingPath ?? "", filePath);
            const segments = rel.split(path.sep);
            const pkgName =
               segments.length > 1 &&
               segments[0] &&
               !segments[0].startsWith("..")
                  ? segments[0]
                  : null;
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

   public getWatchStatus: Handler<void, WatchStatusRes> = async (_req, res) => {
      return res.json({
         enabled: !!this.watchingPath,
         watchingPath: this.watchingPath ?? "",
         environmentName: this.watchingEnvironmentName ?? "",
      });
   };

   public startWatching: Handler<StartWatchReq, { error: string }> = async (
      req,
      res,
   ) => {
      const watchName = req.body.environmentName ?? "";
      try {
         assertSafePackageName(watchName);
      } catch (error) {
         logger.error(error);
         const { status, json } = internalErrorToHttpError(error as Error);
         res.status(status).json({ error: json.message });
         return;
      }
      const environmentManifest =
         await EnvironmentStore.reloadEnvironmentManifest(
            this.environmentStore.serverRootPath,
         );
      const environment = environmentManifest.environments.find(
         (e) => e.name === watchName,
      );
      if (
         !environment ||
         !environment.packages ||
         environment.packages.length === 0
      ) {
         res.status(404).json({
            error: `Environment ${watchName} not found or has no packages`,
         });
         return;
      }
      try {
         await this.ensureWatching(watchName);
      } catch (error) {
         logger.error(error);
         // Take the mapper's message, not error.message: a watch-path
         // EACCES/ENOSPC carries an internal filesystem path.
         const { status, json } = internalErrorToHttpError(error as Error);
         res.status(status).json({ error: json.message });
         return;
      }
      res.json();
   };

   public stopWatchMode: Handler = async (_req, res) => {
      if (this.watcher) {
         await this.watcher.close();
         this.watcher = null;
      }
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
      res.json();
   };
}
