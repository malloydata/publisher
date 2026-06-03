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

// File extensions that should trigger a live-reload event but NOT a Malloy
// environment reload (these are package assets, not semantic-model sources).
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
   watcher: FSWatcher;
   /**
    * Per-package change bus. Event name is `<environmentName>/<packageName>`.
    * Used by the SSE live-reload endpoint to push refreshes to embedded HTML
    * dashboards. Each emit carries `{ path, kind }` for diagnostics; the SSE
    * handler currently ignores the payload and just sends "changed".
    */
   public events = new EventEmitter();

   constructor(private environmentStore: EnvironmentStore) {
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
      // Live-reload subscribers can be many (one per open browser tab);
      // bump the default cap so Node doesn't warn under normal use.
      this.events.setMaxListeners(100);
   }

   /**
    * Idempotent: starts watching `environmentName` if not already.
    *
    * Throws if the environment can't be resolved — never falls back to a
    * `<serverRoot>/<envName>` path that an attacker could control via
    * URL-encoded path traversal. Callers (the SSE handler in particular)
    * must validate the env name before calling this.
    */
   public async ensureWatching(environmentName: string): Promise<void> {
      if (this.watchingEnvironmentName === environmentName && this.watcher) {
         return;
      }
      const env = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const watchPath = env.getEnvironmentPath();
      if (this.watcher) {
         await this.watcher.close();
      }
      this.startWatcher(environmentName, watchPath);
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
      // Reload just the package a changed file belongs to: this recompiles
      // that package's models from disk (Package.create) and replaces the
      // cached entry, so the next query sees the edit. Scoped to watch mode by
      // construction — it only runs from this watcher, which is started only
      // when watch mode is active.
      //
      // The previous env-level reload (getEnvironment(reload) + addEnvironment)
      // was a no-op for compilation: addEnvironment on an existing env calls
      // Environment.update(), which refreshes metadata/connections only and
      // never touches this.packages, so edits never took effect.
      // Returns true if the package recompiled cleanly. A transient compile
      // error (e.g. a half-typed model saved mid-edit) returns false so we can
      // avoid bouncing open browser tabs into a compile-error/404 — see onEvent.
      const reloadPackage = async (pkgName: string): Promise<boolean> => {
         try {
            const environment = await this.environmentStore.getEnvironment(
               watchName,
               false,
            );
            await environment.getPackage(pkgName, true);
            logger.info(
               `Watch: recompiled package "${pkgName}" in environment "${watchName}"`,
            );
            return true;
         } catch (error) {
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
            // Resolve which package the file belongs to. Packages are
            // subdirectories of the environment (env/<pkg>/...), so a file must
            // be nested at least one level to belong to a package; files at the
            // environment root belong to no package and are ignored.
            const rel = path.relative(this.watchingPath ?? "", filePath);
            const segments = rel.split(path.sep);
            const pkgName =
               segments.length > 1 &&
               segments[0] &&
               !segments[0].startsWith("..")
                  ? segments[0]
                  : null;
            if (!pkgName) return;

            // Recompile Malloy state only for model files. Asset edits
            // (HTML/CSS/JS) skip recompile — they just need the live-reload
            // fanout below. For model edits, only signal a reload once the
            // recompile succeeds: a transient syntax error shouldn't bounce
            // open pages into a compile error or 404.
            const ext = path.extname(filePath).toLowerCase();
            if (MODEL_EXTS.has(ext)) {
               const recompiled = await reloadPackage(pkgName);
               if (!recompiled) return;
            }
            // Fan out to SSE clients embedded in the affected package.
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
         const { status } = internalErrorToHttpError(error as Error);
         res.status(status).json({ error: (error as Error).message });
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
      await this.ensureWatching(watchName);
      res.json();
   };

   public stopWatchMode: Handler = async (_req, res) => {
      if (this.watcher) {
         await this.watcher.close();
      }
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
      res.json();
   };
}
