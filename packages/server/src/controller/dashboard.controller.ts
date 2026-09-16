// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { components } from "../api";
import {
   BadRequestError,
   DashboardNotFoundError,
   FrozenConfigError,
   WriteConflictError,
   WriteRolledBackError,
} from "../errors";
import { logger } from "../logger";
import { assertSafeRelativeModelPath } from "../path_safety";
import { EnvironmentStore } from "../service/environment_store";

type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];
type ApiModelSourceWrite = components["schemas"]["ModelSourceWriteRequest"];
type ApiModelSourceWriteResult =
   components["schemas"]["ModelSourceWriteResult"];

/** The only files the write endpoint accepts: a dashboard, at the top of `dashboards/`. */
const DASHBOARD_FILE = /^dashboards\/[^/]+\.malloy$/;

/**
 * One compile problem, as a caller can act on it: where it is, then what it is.
 * The location is the whole reason to say more than the message — an author
 * looking at a refused save wants the line, and the endpoint answers with an
 * `Error` body rather than the `/compile` endpoint's structured problems.
 */
const describeProblem = (problem: {
   message: string;
   at?: { range?: { start?: { line?: number; character?: number } } };
}): string => {
   const start = problem.at?.range?.start;
   return start?.line === undefined
      ? problem.message
      : `line ${start.line + 1}:${(start.character ?? 0) + 1} ${problem.message}`;
};

/** SHA-256 of a file's text, hex: what a caller hands back as `expectedHash`. */
export const contentHashOf = (text: string): string =>
   createHash("sha256").update(text, "utf8").digest("hex");

/**
 * Read-only discovery for a package's dashboards. Both routes serve state the
 * package computed at load, so neither compiles or queries anything.
 *
 * There is deliberately no run endpoint: a dashboard's query, a composite's
 * tiles, and a control's suggest query all run through the ordinary
 * `POST …/models/{path}/query` with `givens` — the same governed path every
 * other query takes, so row caps, byte caps, authorize gates, and render-tag
 * validation apply for free.
 */
export class DashboardController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listDashboards(
      environmentName: string,
      packageName: string,
   ): Promise<ApiDashboard[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listDashboards();
   }

   public async getDashboard(
      environmentName: string,
      packageName: string,
      dashboardName: string,
   ): Promise<ApiDashboardManifest> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const dashboard = p.getDashboard(dashboardName);
      if (!dashboard) {
         throw new DashboardNotFoundError(
            `Dashboard ${dashboardName} does not exist in package ${packageName}`,
         );
      }
      return dashboard;
   }

   /**
    * Write a dashboard file into the package and serve it: the builder's save.
    *
    * In order — refuse under `frozenConfig`; accept only `dashboards/<slug>.malloy`;
    * compile the text AS the file and refuse with the problems when it does not
    * compile, writing nothing; then, under one hold of the package lock, check
    * the caller's precondition and write atomically; reload the package in
    * place; and if the reloaded package does not compile this file, put the
    * previous text back (or remove a new file) and reload again, so a save
    * never leaves the package serving less than it did.
    *
    * The precondition is `expectedHash`: the hash of the text the caller
    * opened, refused with 409 when the file has changed since, and nothing
    * merged. Omitting it means "create": a file that already exists is refused
    * the same way, so an unconditional overwrite is not something a caller can
    * ask for by leaving a field out.
    *
    * Compiling happens BEFORE the lock because the lock is not reentrant and
    * compiling reads the package; it does not read the file being replaced, so
    * nothing about the check depends on it.
    */
   public async putDashboardSource(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: ApiModelSourceWrite,
   ): Promise<ApiModelSourceWriteResult> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError(
            'Cannot write a dashboard: publisher.config.json has "frozenConfig": true.',
         );
      }
      assertSafeRelativeModelPath(modelPath);
      if (!DASHBOARD_FILE.test(modelPath)) {
         throw new BadRequestError(
            `Only a dashboard file can be written here: \`dashboards/<slug>.malloy\`, ` +
               `not \`${modelPath}\`.`,
         );
      }
      if (typeof body?.source !== "string") {
         throw new BadRequestError(
            "The request body needs a `source`: the whole file's Malloy text.",
         );
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Loads the package if it is not yet, and is the 404 for one that does
      // not exist.
      await environment.getPackage(packageName, false);

      const { problems } = await environment.compileSource(
         packageName,
         modelPath,
         body.source,
         false,
         undefined,
         "file",
      );
      const errors = problems.filter((problem) => problem.severity === "error");
      if (errors.length > 0) {
         throw new BadRequestError(
            `The dashboard does not compile, so it was not written: ` +
               errors.map(describeProblem).join("; "),
         );
      }

      const { previous } = await environment.writeModelFileChecked(
         packageName,
         modelPath,
         body.source,
         (current) => {
            if (body.expectedHash === undefined) {
               if (current !== undefined)
                  throw new WriteConflictError(
                     `\`${modelPath}\` already exists in the package. Send the ` +
                        `\`expectedHash\` of the text you opened to replace it.`,
                  );
               return;
            }
            const currentHash =
               current === undefined ? undefined : contentHashOf(current);
            if (currentHash !== body.expectedHash)
               throw new WriteConflictError(
                  current === undefined
                     ? `\`${modelPath}\` no longer exists in the package, so the text you ` +
                       `opened cannot be updated. Re-open it before saving.`
                     : `\`${modelPath}\` changed in the package since you opened it. ` +
                       `Re-open it and reapply your change; nothing was written.`,
               );
         },
      );
      try {
         const reloaded = await environment.getPackage(packageName, true);
         // A reload that fails to compile keeps the last good model serving
         // and marks the package stale rather than throwing, so the file just
         // written is asked for directly.
         const written = reloaded.getModel(modelPath);
         if (!written)
            throw new Error(`\`${modelPath}\` is not in the reloaded package`);
         await written.getModel();
      } catch (error) {
         await environment.restoreModelFile(packageName, modelPath, previous);
         await environment.getPackage(packageName, true).catch(() => undefined);
         logger.error("Dashboard write rolled back", {
            packageName,
            modelPath,
            error,
         });
         throw new WriteRolledBackError(
            `The package did not reload with the new \`${modelPath}\`, so the ` +
               `previous text was put back and nothing changed.`,
         );
      }
      return {
         resource: `/api/v0/environments/${environmentName}/packages/${packageName}/models/${modelPath}`,
         path: modelPath,
         contentHash: contentHashOf(body.source),
         created: previous === undefined,
      };
   }
}
