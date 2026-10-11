// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { components } from "../api";
import {
   BadRequestError,
   CompileRefusedError,
   DashboardNotFoundError,
   FrozenConfigError,
   WriteConflictError,
   WriteRolledBackError,
   WriteVerifyError,
} from "../errors";
import {
   recordDashboardWrite,
   type ReportWriteLayout,
   type DashboardWriteOutcome,
} from "../dashboard_write_metrics";
import { assertSafeRelativeModelPath } from "../path_safety";
import { INDEX_MODEL_NAME, MODEL_FILE_SUFFIX } from "../constants";
import {
   carriesArtifactTag,
   isUnderPublicDir,
   readReportTagOfText,
} from "../service/report";
import { formatProblem } from "../service/query_text";
import { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";

type ApiReport = components["schemas"]["Report"];
type ApiReportManifest = components["schemas"]["ReportManifest"];
type ApiModelSourceWrite = components["schemas"]["ModelSourceWriteRequest"];
type ApiModelSourceWriteResult =
   components["schemas"]["ModelSourceWriteResult"];

/**
 * Whether the file now at `modelPath` is one the write path may replace: a
 * `.malloy` whose on-disk text, which may postdate the load, carries a
 * model-level artifact tag, and which the loaded package reads as a report
 * (served, or a grid report with no tiles yet) or which claims to be one by
 * its own `## artifact` note even though that tag does not parse or sets the
 * removed `kind=`. The last is still the author's report, and saving the fix
 * is how it gets served again. A model, an untagged file, a `.malloynb`, the
 * root `index.malloy` and anything in `public/` never qualify (the last three
 * are refused by path before this runs).
 */
function currentIsAReport(
   current: string,
   loaded: Package | undefined,
   modelPath: string,
): boolean {
   return (
      carriesArtifactTag(current) &&
      loaded?.isWritableReport(modelPath) === true
   );
}

/**
 * Which outcome an error from the write path represents.
 *
 * Read from the error's TYPE rather than decided at each throw site, so a
 * branch added later is classified by what it raises instead of being silently
 * counted as something else — or forgotten. The three named types are the
 * three refusals worth telling apart; everything else, from a frozen config to
 * a 404 to something genuinely unexpected, shares one bucket because it shares
 * the only fact an operator needs from it: nothing was written.
 */
function outcomeOf(error: Error): DashboardWriteOutcome {
   if (error instanceof WriteConflictError) return "conflict";
   if (error instanceof CompileRefusedError) return "compile_failed";
   if (error instanceof WriteRolledBackError) return "rolled_back";
   return "refused";
}

/**
 * Where a report may be created: any `.malloy` path in the package outside
 * `public/` (served raw to anyone who can reach the server) other than the
 * root `index.malloy` (the surface, never a report). Where it lives otherwise
 * decides nothing.
 */
function reportPathProblem(modelPath: string): string | undefined {
   if (!modelPath.endsWith(MODEL_FILE_SUFFIX))
      return `Only a report, a \`.malloy\` file, can be written here, not \`${modelPath}\`.`;
   if (isUnderPublicDir(modelPath))
      return `\`${modelPath}\` is in \`public/\`, which is served as raw static files, so a report cannot be written there.`;
   if (modelPath === INDEX_MODEL_NAME)
      return `\`${INDEX_MODEL_NAME}\` at the package root is the package's surface, never a report, so it cannot be written here.`;
   return undefined;
}

/** SHA-256 of a file's text, hex: what a caller hands back as `expectedHash`. */
export const contentHashOf = (text: string): string =>
   createHash("sha256").update(text, "utf8").digest("hex");

/**
 * A package's reports: the two read routes serve state the package computed
 * at load, so neither compiles or queries anything, and the write route is the
 * builder's save.
 *
 * There is deliberately no run endpoint: a report's tiles, cells and a
 * control's suggest query all run through the ordinary
 * `POST …/models/{path}/query` with `givens` — the same governed path every
 * other query takes, so row caps, byte caps, authorize gates, and render-tag
 * validation apply for free.
 */
export class DashboardController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   /** Every report in the package, of either layout, by path. */
   public async listReports(
      environmentName: string,
      packageName: string,
   ): Promise<ApiReport[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listReports();
   }

   /** One report by its path in the package. */
   public async getReport(
      environmentName: string,
      packageName: string,
      reportPath: string,
   ): Promise<ApiReportManifest> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const report =
         typeof reportPath === "string" && reportPath.length > 0
            ? await p.getReport(reportPath)
            : undefined;
      if (!report) {
         throw new DashboardNotFoundError(
            `Report ${reportPath} does not exist in package ${packageName}`,
         );
      }
      return report;
   }

   /**
    * Write a report file into the package and serve it: the builder's save
    * (`docs/reports-design.md` §7).
    *
    * In order — refuse under `frozenConfig`; accept a safe `.malloy` path
    * outside `public/` and other than the root `index.malloy`, whose text
    * carries an artifact tag; compile the text AS the file and refuse with the
    * problems when it does not compile, writing nothing; then, under one hold
    * of the package lock, check the caller's precondition (a create needs no
    * file there; a replace needs a file the package reads as a report) and
    * write atomically, creating missing directories inside the package root
    * only; reload the package in place; and if the reloaded package does not
    * serve this file as a report, put the previous text back (or remove a new
    * file) and reload again, so a save never leaves the package serving less
    * than it did.
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
      // One record per attempt, whichever way it leaves — including the throws,
      // which are most of what is worth knowing here. Classified from the error
      // rather than at each throw site, so a branch added later cannot forget.
      const startedAt = Date.now();
      // Tolerates a non-string path or source: the 400 for either comes from
      // writeDashboardSource. A body the cheap refusals turn away is never
      // scanned, and counts as a grid write, as does text that is no report.
      const tag =
         typeof modelPath === "string" &&
         typeof body?.source === "string" &&
         !this.environmentStore.publisherConfigIsFrozen &&
         reportPathProblem(modelPath) === undefined
            ? readReportTagOfText(body.source)
            : undefined;
      const layout: ReportWriteLayout =
         tag?.status === "report" ? tag.layout : "grid";
      try {
         const result = await this.writeDashboardSource(
            environmentName,
            packageName,
            modelPath,
            body,
         );
         recordDashboardWrite(
            result.created ? "created" : "replaced",
            Date.now() - startedAt,
            layout,
         );
         return result;
      } catch (error) {
         recordDashboardWrite(
            outcomeOf(error as Error),
            Date.now() - startedAt,
            layout,
         );
         throw error;
      }
   }

   private async writeDashboardSource(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: ApiModelSourceWrite,
   ): Promise<ApiModelSourceWriteResult> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError(
            'Cannot write a report: publisher.config.json has "frozenConfig": true.',
         );
      }
      assertSafeRelativeModelPath(modelPath);
      const pathProblem = reportPathProblem(modelPath);
      if (pathProblem) throw new BadRequestError(pathProblem);
      if (typeof body?.source !== "string") {
         throw new BadRequestError(
            "The request body needs a `source`: the whole file's Malloy text.",
         );
      }
      // An untagged file is a model that other files may import, not a
      // report. A tag that does not parse, or that sets `kind=`, passes here so
      // the compile below refuses it with the lint's fix.
      if (!carriesArtifactTag(body.source)) {
         throw new BadRequestError(
            `\`${modelPath}\` has no artifact tag (\`## artifact { layout=grid … }\` ` +
               `or \`## artifact { layout=linear … }\`), so it is not a report. ` +
               `Only a report can be written here.`,
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
         throw new CompileRefusedError(
            `The report does not compile, so it was not written: ` +
               errors.map(formatProblem).join("; "),
         );
      }

      // One hold of the package lock covers the precondition, the write, the
      // reload and the restore. Compiling above stays outside it: the lock is
      // not reentrant, and the proposed text compiles the same whatever is on
      // disk.
      const { previous } = await environment.writeModelFileTransactional(
         packageName,
         modelPath,
         body.source,
         (current, loaded) => {
            // A create needs the path free. Any file there, report or not,
            // is a conflict, so a caller choosing a name can try the next.
            if (body.expectedHash === undefined) {
               if (current !== undefined)
                  throw new WriteConflictError(
                     `\`${modelPath}\` already exists in the package, so nothing ` +
                        `was written. Choose another path, or, to replace a report, ` +
                        `send the \`expectedHash\` of the text you opened.`,
                  );
               return;
            }
            // The incoming text's tag is not enough: a tagged write must not
            // turn an existing model into a report. Only a file the package
            // reads as a report is replaced, wherever it lives.
            if (
               current !== undefined &&
               !currentIsAReport(current, loaded, modelPath)
            )
               throw new BadRequestError(
                  `\`${modelPath}\` exists in the package and is not a report: ` +
                     `it is a model (or a file other models import), so it was ` +
                     `not overwritten.`,
               );
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
         // A reload that fails to compile keeps the last good model serving
         // and marks the package stale rather than throwing, so the file just
         // written is asked for directly.
         async (reloaded) => {
            const written = reloaded.getModel(modelPath);
            if (!written)
               throw new WriteVerifyError(
                  `\`${modelPath}\` is not in the reloaded package`,
               );
            await written.getModel().catch((cause) => {
               throw new WriteVerifyError(
                  `\`${modelPath}\` did not compile once the package reloaded`,
                  { cause },
               );
            });
            // A grid report with no tiles yet is the builder's to save but not
            // served, so "a report" is the test, not "served".
            if (!reloaded.isReport(modelPath))
               throw new WriteVerifyError(
                  `\`${modelPath}\` is not served as a report once the package reloads`,
               );
         },
      );
      return {
         resource: `/api/v0/environments/${environmentName}/packages/${packageName}/models/${modelPath}`,
         path: modelPath,
         contentHash: contentHashOf(body.source),
         created: previous === undefined,
      };
   }
}
