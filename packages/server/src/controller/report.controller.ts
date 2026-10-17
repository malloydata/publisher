// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { components } from "../api";
import {
   BadRequestError,
   CompileRefusedError,
   DashboardNotFoundError,
   EnvironmentNotFoundError,
   FrozenConfigError,
   InvalidModelPathError,
   PackageNotFoundError,
   PackageVersionError,
   WriteConflictError,
   WriteRollbackFailedError,
   WriteRolledBackError,
   WriteVerifyError,
} from "../errors";
import {
   recordReportWrite,
   type ReportLayout,
   type ReportWriteOutcome,
   type ReportWriteReason,
} from "../report_metrics";
import { assertSafeRelativeModelPath } from "../path_safety";
import {
   carriesArtifactTag,
   readReportTagOfText,
   reportWritePathProblem,
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
 * Whether the write path may replace the file at `modelPath`: its on-disk text
 * carries an artifact tag and the loaded package reads it as a report (served,
 * tileless, or invalid, since saving the fix is how an invalid one recovers).
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

/** A 400 raised by the write path, carrying the `reason` it is counted under. */
class ReportWriteRefusedError extends BadRequestError {
   constructor(
      readonly reason: ReportWriteReason,
      message: string,
   ) {
      super(message);
   }
}

/**
 * The outcome and reason a write-path error is counted under. Read from the
 * error's type rather than set at each throw site, so a new branch cannot be
 * forgotten. Every refusal shares outcome `refused` (nothing was written).
 */
function outcomeOf(error: Error): {
   outcome: ReportWriteOutcome;
   reason: ReportWriteReason;
} {
   if (error instanceof WriteConflictError)
      return { outcome: "conflict", reason: "conflict" };
   if (error instanceof CompileRefusedError)
      return { outcome: "compile_failed", reason: "compile_failed" };
   if (error instanceof WriteRollbackFailedError)
      return { outcome: "rollback_failed", reason: "reload_failed" };
   if (error instanceof WriteRolledBackError)
      return { outcome: "rolled_back", reason: "reload_failed" };
   const reason: ReportWriteReason =
      error instanceof ReportWriteRefusedError
         ? error.reason
         : error instanceof InvalidModelPathError
           ? "invalid_path"
           : error instanceof FrozenConfigError
             ? "frozen_config"
             : error instanceof EnvironmentNotFoundError ||
                 error instanceof PackageNotFoundError
               ? "not_found"
               : error instanceof BadRequestError
                 ? "bad_request"
                 : "other";
   return { outcome: "refused", reason };
}

/**
 * The version option for a package read: none when the caller names no
 * version, so an unversioned read asks for the package exactly as before.
 */
export const versionOptions = (
   versionId: unknown,
): [{ versionId: unknown }] | [] =>
   versionId === undefined ? [] : [{ versionId }];

/** SHA-256 of a file's text, hex: what a caller hands back as `expectedHash`. */
export const contentHashOf = (text: string): string =>
   createHash("sha256").update(text, "utf8").digest("hex");

/**
 * A package's reports: two read routes over load-time state, and the builder's
 * save. There is deliberately no run endpoint: tiles run through the ordinary
 * `POST …/models/{path}/query` with `givens`, so every query governance check
 * applies.
 */
export class ReportController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   /** Every report in the package, of either layout, by path. */
   public async listReports(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
   ): Promise<ApiReport[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(
         packageName,
         false,
         ...versionOptions(versionId),
      );
      return p.listReports();
   }

   /** One report by its path in the package. */
   public async getReport(
      environmentName: string,
      packageName: string,
      reportPath: string,
      versionId?: unknown,
   ): Promise<ApiReportManifest> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(
         packageName,
         false,
         ...versionOptions(versionId),
      );
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
    * (docs/reports.md, "The write path"). Compiles the text as the file and
    * refuses without writing if it fails; then, under one hold of the package
    * lock, checks the precondition, writes atomically and reloads, restoring
    * the previous text if the reload does not serve the file as a report, so
    * a save never leaves the package serving less than it did.
    *
    * `expectedHash` is the hash of the text the caller opened (409 if it
    * changed). Omitting it means "create", which also 409s on an existing
    * file, so no caller gets an unconditional overwrite by leaving it out.
    */
   public async putReportSource(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: ApiModelSourceWrite,
      versionId?: unknown,
   ): Promise<ApiModelSourceWriteResult> {
      // One record per attempt, including the throws.
      const startedAt = Date.now();
      // Tolerates a non-string path or source (writeReportSource 400s). A body
      // the cheap refusals turn away is not scanned and counts as grid.
      const tag =
         typeof modelPath === "string" &&
         typeof body?.source === "string" &&
         !this.environmentStore.publisherConfigIsFrozen &&
         reportWritePathProblem(modelPath) === undefined
            ? readReportTagOfText(body.source)
            : undefined;
      const layout: ReportLayout =
         tag?.status === "report" ? tag.layout : "grid";
      try {
         const result = await this.writeReportSource(
            environmentName,
            packageName,
            modelPath,
            body,
            versionId,
         );
         recordReportWrite(
            result.created ? "created" : "replaced",
            "none",
            Date.now() - startedAt,
            layout,
         );
         return result;
      } catch (error) {
         const { outcome, reason } = outcomeOf(error as Error);
         recordReportWrite(outcome, reason, Date.now() - startedAt, layout);
         throw error;
      }
   }

   private async writeReportSource(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: ApiModelSourceWrite,
      versionId?: unknown,
   ): Promise<ApiModelSourceWriteResult> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError(
            'Cannot write a report: publisher.config.json has "frozenConfig": true.',
         );
      }
      try {
         assertSafeRelativeModelPath(modelPath);
      } catch (error) {
         throw new ReportWriteRefusedError(
            "invalid_path",
            (error as Error).message,
         );
      }
      const pathProblem = reportWritePathProblem(modelPath);
      if (pathProblem)
         throw new ReportWriteRefusedError("invalid_path", pathProblem);
      if (typeof body?.source !== "string") {
         throw new ReportWriteRefusedError(
            "missing_source",
            "The request body needs a `source`: the whole file's Malloy text.",
         );
      }
      // An untagged file is a model that other files may import, not a
      // report. A tag that does not parse, or that sets `kind=`, passes here so
      // the compile below refuses it with the lint's fix.
      if (!carriesArtifactTag(body.source)) {
         throw new ReportWriteRefusedError(
            "untagged",
            `\`${modelPath}\` has no artifact tag (\`## artifact { layout=grid … }\` ` +
               `or \`## artifact { layout=linear … }\`), so it is not a report. ` +
               `Only a report can be written here.`,
         );
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // A published version is immutable, so a package that has versions takes
      // no in-place write whatever version is named; one with none has no
      // version to name (404).
      const versions = environment.getVersionService();
      if (versions && (await versions.isVersioned(packageName))) {
         throw new PackageVersionError(
            "PACKAGE_IS_VERSIONED",
            `Package ${packageName} has published versions, which are immutable. Change the file in the package's source and publish a new version.`,
         );
      }
      await versions?.resolve(packageName, versionId);
      // Loads the package, or 404s.
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

      // Compiling stays outside the package lock: the lock is not reentrant,
      // and the proposed text compiles the same whatever is on disk.
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
            // A tagged write must not turn an existing model into a report.
            if (
               current !== undefined &&
               !currentIsAReport(current, loaded, modelPath)
            )
               throw new ReportWriteRefusedError(
                  "not_a_report",
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
