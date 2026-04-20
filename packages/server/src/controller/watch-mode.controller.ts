import chokidar, { FSWatcher } from "chokidar";
import { RequestHandler } from "express";
import path from "path";
import { components } from "../api";
import { logger } from "../logger";
import { ProjectStore } from "../service/project_store";

type StartWatchReq = components["schemas"]["StartWatchRequest"];
type WatchStatusRes = components["schemas"]["WatchStatus"];
type Handler<Req = object, Res = void> = RequestHandler<object, Res, Req>;

export class WatchModeController {
   watchingPath: string | null;
   watchingProjectName: string | null;
   watcher: FSWatcher;

   constructor(private projectStore: ProjectStore) {
      this.watchingPath = null;
      this.watchingProjectName = null;
   }

   public getWatchStatus: Handler<void, WatchStatusRes> = async (_req, res) => {
      const name = this.watchingProjectName ?? "";
      return res.json({
         enabled: !!this.watchingPath,
         watchingPath: this.watchingPath ?? "",
         projectName: name,
         environmentName: name,
      });
   };

   public startWatching: Handler<StartWatchReq, { error: string }> = async (
      req,
      res,
   ) => {
      const watchName = req.body.projectName ?? req.body.environmentName ?? "";
      const projectManifest = await ProjectStore.reloadProjectManifest(
         this.projectStore.serverRootPath,
      );
      this.watchingProjectName = watchName || null;

      // Find the environment in the manifest ("projects" array)
      const project = projectManifest.projects.find(
         (p) => p.name === watchName,
      );
      if (!project || !project.packages || project.packages.length === 0) {
         res.status(404).json({
            error: `Environment ${watchName} not found or has no packages`,
         });
         return;
      }

      this.watchingPath = path.join(
         this.projectStore.serverRootPath,
         watchName,
      );
      this.watcher = chokidar.watch(this.watchingPath, {
         ignored: (path, stats) =>
            !!stats?.isFile() &&
            !path.endsWith(".malloy") &&
            !path.endsWith(".md"),
         ignoreInitial: true,
      });
      const reloadProject = async () => {
         // Overwrite the environment with its existing metadata to trigger a re-read
         const project = await this.projectStore.getProject(watchName, true);
         await this.projectStore.addProject(project.metadata);
         logger.info(`Reloaded environment ${watchName}`);
      };

      this.watcher.on("add", async (path) => {
         logger.info(
            `Detected new file ${path}, reloading environment ${watchName}`,
         );
         await reloadProject();
      });
      this.watcher.on("unlink", async (path) => {
         logger.info(
            `Detected deletion of ${path}, reloading environment ${watchName}`,
         );
         await reloadProject();
      });
      this.watcher.on("change", async (path) => {
         logger.info(
            `Detected change on ${path}, reloading environment ${watchName}`,
         );
         await reloadProject();
      });
      res.json();
   };

   public stopWatchMode: Handler = async (_req, res) => {
      this.watcher.close();
      this.watchingPath = null;
      this.watchingProjectName = null;
      res.json();
   };
}
