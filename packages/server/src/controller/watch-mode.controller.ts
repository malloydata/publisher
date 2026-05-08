import chokidar, { FSWatcher } from "chokidar";
import { RequestHandler } from "express";
import path from "path";
import { components } from "../api";
import { logger } from "../logger";
import { EnvironmentStore } from "../service/environment_store";

type StartWatchReq = components["schemas"]["StartWatchRequest"];
type WatchStatusRes = components["schemas"]["WatchStatus"];
type Handler<Req = object, Res = void> = RequestHandler<object, Res, Req>;

export class WatchModeController {
   watchingPath: string | null;
   watchingEnvironmentName: string | null;
   watcher: FSWatcher;

   constructor(private environmentStore: EnvironmentStore) {
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
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
      const environmentManifest =
         await EnvironmentStore.reloadEnvironmentManifest(
            this.environmentStore.serverRootPath,
         );
      this.watchingEnvironmentName = watchName || null;

      // Find the environment in the manifest
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

      this.watchingPath = path.join(
         this.environmentStore.serverRootPath,
         watchName,
      );
      this.watcher = chokidar.watch(this.watchingPath, {
         ignored: (path, stats) =>
            !!stats?.isFile() &&
            !path.endsWith(".malloy") &&
            !path.endsWith(".md"),
         ignoreInitial: true,
      });
      const reloadEnvironment = async () => {
         // Overwrite the environment with its existing metadata to trigger a re-read
         const environment = await this.environmentStore.getEnvironment(
            watchName,
            true,
         );
         await this.environmentStore.addEnvironment(environment.metadata);
         logger.info(`Reloaded environment ${watchName}`);
      };

      this.watcher.on("add", async (path) => {
         logger.info(
            `Detected new file ${path}, reloading environment ${watchName}`,
         );
         await reloadEnvironment();
      });
      this.watcher.on("unlink", async (path) => {
         logger.info(
            `Detected deletion of ${path}, reloading environment ${watchName}`,
         );
         await reloadEnvironment();
      });
      this.watcher.on("change", async (path) => {
         logger.info(
            `Detected change on ${path}, reloading environment ${watchName}`,
         );
         await reloadEnvironment();
      });
      res.json();
   };

   public stopWatchMode: Handler = async (_req, res) => {
      this.watcher.close();
      this.watchingPath = null;
      this.watchingEnvironmentName = null;
      res.json();
   };
}
