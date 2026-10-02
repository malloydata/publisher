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
   type DashboardWriteKind,
   type DashboardWriteOutcome,
} from "../dashboard_write_metrics";
import { assertSafeRelativeModelPath } from "../path_safety";
import {
   artifactKindInText,
   claimsToBeANotebook,
   documentKind,
   hasArtifactLineOutsideBlocks,
} from "../service/notebook";
import { dashboardSlug, factsCarryArtifactTag } from "../service/dashboard";
import { formatProblem } from "../service/query_text";
import { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";

type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];
type ApiModelSourceWrite = components["schemas"]["ModelSourceWriteRequest"];
type ApiModelSourceWriteResult =
   components["schemas"]["ModelSourceWriteResult"];

/** Tagged document as discovery judges it (a tag in a comment is no model note), and by its on-disk text too, which may postdate the load. */
function currentIsATaggedDocument(
   current: string,
   loaded: Package | undefined,
   modelPath: string,
): boolean {
   const model = loaded?.getModel(modelPath);
   if (model?.getModelDef() && !model.carriesNotebookArtifactNote())
      return false;
   return claimsToBeANotebook(current);
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

/** The only files the write endpoint accepts: a dashboard or a notebook, at the top of its directory. */
const DASHBOARD_FILE = /^(dashboards|notebooks)\/[^/]+\.malloy$/;

/** An `# artifact` or `## artifact` line, or the opener of a block holding one. */
const ANY_ARTIFACT_NOTE = /^#{1,2}(?:\|\s*|[ \t]*)artifact\b/;

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
    * Write a dashboard or notebook file into the package and serve it: the builder's save.
    *
    * In order — refuse under `frozenConfig`; accept only `dashboards/<slug>.malloy`
    * or a tagged `notebooks/<slug>.malloy`;
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
      // One record per attempt, whichever way it leaves — including the throws,
      // which are most of what is worth knowing here. Classified from the error
      // rather than at each throw site, so a branch added later cannot forget.
      const startedAt = Date.now();
      // Tolerates a non-string path or source: the 400 for either comes from writeDashboardSource.
      const kind: DashboardWriteKind =
         typeof modelPath === "string" && typeof body?.source === "string"
            ? documentKind(modelPath, artifactKindInText(body.source))
            : "dashboard";
      try {
         const result = await this.writeDashboardSource(
            environmentName,
            packageName,
            modelPath,
            body,
            kind,
         );
         recordDashboardWrite(
            result.created ? "created" : "replaced",
            Date.now() - startedAt,
            kind,
         );
         return result;
      } catch (error) {
         recordDashboardWrite(
            outcomeOf(error as Error),
            Date.now() - startedAt,
            kind,
         );
         throw error;
      }
   }

   private async writeDashboardSource(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: ApiModelSourceWrite,
      kind: DashboardWriteKind,
   ): Promise<ApiModelSourceWriteResult> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError(
            'Cannot write a dashboard or notebook: publisher.config.json has "frozenConfig": true.',
         );
      }
      assertSafeRelativeModelPath(modelPath);
      if (!DASHBOARD_FILE.test(modelPath)) {
         throw new BadRequestError(
            `Only a dashboard or a notebook file can be written here: ` +
               `\`dashboards/<slug>.malloy\` or \`notebooks/<slug>.malloy\`, ` +
               `not \`${modelPath}\`.`,
         );
      }
      if (typeof body?.source !== "string") {
         throw new BadRequestError(
            "The request body needs a `source`: the whole file's Malloy text.",
         );
      }
      // An untagged file under notebooks/ is a shared include that other models import.
      const inNotebooksFolder = modelPath.startsWith("notebooks/");
      if (inNotebooksFolder && !claimsToBeANotebook(body.source)) {
         throw new BadRequestError(
            `\`${modelPath}\` has no \`## artifact\` tag, so it is not a notebook. ` +
               `Only a dashboard (\`dashboards/<slug>.malloy\`) or a tagged notebook ` +
               `(\`notebooks/<slug>.malloy\`) can be written here.`,
         );
      }
      // A dashboard may be tagged at the query level (`#`), so either sigil passes; a tag in a comment or string still reaches the reload verify.
      if (
         !inNotebooksFolder &&
         !hasArtifactLineOutsideBlocks(body.source, ANY_ARTIFACT_NOTE)
      ) {
         throw new BadRequestError(
            `\`${modelPath}\` has no \`# artifact\` tag, so it is not a dashboard.`,
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
            `The ${kind} does not compile, so it was not written: ` +
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
            // The incoming text's tag is not enough: a tagged write must not turn an
            // existing shared include into a notebook.
            if (
               inNotebooksFolder &&
               current !== undefined &&
               !currentIsATaggedDocument(current, loaded, modelPath)
            )
               throw new BadRequestError(
                  `\`${modelPath}\` exists in the package without an \`## artifact\` tag: ` +
                     `it is a shared include that other models import, not a document, ` +
                     `so it was not overwritten.`,
               );
            const slug = dashboardSlug(modelPath);
            const holder = loaded?.getDashboard(slug)?.path;
            if (kind === "dashboard" && holder && holder !== modelPath)
               throw new WriteConflictError(
                  `\`${modelPath}\` would not be served: \`${holder}\` already holds the ` +
                     `dashboard name "${slug}", which is the URL and the \`# drill\` target. ` +
                     `Fix: rename one of the files. Nothing was written.`,
               );
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
            // The textual tag check can pass for a file discovery then drops (a tag in a comment, a slug another file holds).
            let served: boolean;
            if (kind === "dashboard") {
               const holder = reloaded.getDashboard(dashboardSlug(modelPath));
               // A tile-less dashboard yields no manifest, so the compiled tag is the evidence unless another file holds the slug.
               const facts = written.getDashboardModelFacts();
               served = holder
                  ? holder.path === modelPath
                  : facts !== undefined && factsCarryArtifactTag(facts);
            } else served = reloaded.isServedNotebook(modelPath);
            if (!served)
               throw new WriteVerifyError(
                  `\`${modelPath}\` is not served as a ${kind} once the package reloads`,
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
