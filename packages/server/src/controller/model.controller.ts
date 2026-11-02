// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import {
   ModelNotFoundError,
   NotQueryableError,
   PackageVersionError,
} from "../errors";
import { logger } from "../logger";
import { EnvironmentStore } from "../service/environment_store";

type ApiModel = components["schemas"]["Model"];
type ApiCompiledModel = components["schemas"]["CompiledModel"];

/**
 * `includeHiddenFilesAndSources`: also show the files a package's surface hides. It
 * changes what the model routes show, never what a query can reach, and
 * Publisher does not decide who may ask: the gateway in front of it does.
 */
export interface ModelReadOptions {
   includeHiddenFilesAndSources?: boolean;
   /** The published version to read; absent, empty or null means `latest`. */
   versionId?: unknown;
}

export class ModelController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listModels(
      environmentName: string,
      packageName: string,
      options: ModelReadOptions = {},
   ): Promise<ApiModel[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false, {
         versionId: options.versionId,
      });
      return p.listModels(options);
   }

   public async getModel(
      environmentName: string,
      packageName: string,
      modelPath: string,
      options: ModelReadOptions = {},
   ): Promise<ApiCompiledModel> {
      const includeHiddenFilesAndSources =
         options.includeHiddenFilesAndSources === true;
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const p = await environment.getPackage(packageName, false, {
            versionId: options.versionId,
         });
         const model = p.getModel(modelPath);
         if (!model) {
            throw new ModelNotFoundError(`${modelPath} does not exist`);
         }
         if (model.getType() === "notebook") {
            throw new ModelNotFoundError(`${modelPath} is a notebook`);
         }
         // A file nobody can query is not shown either: same rule, same 404
         // text as the query route.
         if (!includeHiddenFilesAndSources) model.assertFileOnSurface();
         // The file text is read without the package lock (callers reachable
         // from `Model.getModel()` must not take it): a mid-swap read sees the
         // old file, the new one, or ENOENT, and ENOENT only withholds the text.
         const [compiled, sourceText] = await Promise.all([
            model.getModel(),
            // Withheld when the text names a source the file does not publish,
            // unless the caller asked to see the files off the surface.
            p
               .getModelFileText(modelPath)
               .then((text) =>
                  includeHiddenFilesAndSources || model.showsFileText(text)
                     ? text
                     : undefined,
               )
               .catch((error: unknown) => {
                  logger.warn("getModel: model source text unavailable", {
                     environmentName,
                     packageName,
                     modelPath,
                     error:
                        error instanceof Error ? error.message : String(error),
                  });
                  return undefined;
               }),
         ]);
         return sourceText === undefined
            ? compiled
            : { ...compiled, sourceText };
      } catch (error) {
         // Re-throw these as-is, so they keep their 404.
         if (
            error instanceof ModelNotFoundError ||
            error instanceof NotQueryableError ||
            error instanceof PackageVersionError
         ) {
            throw error;
         }
         throw new Error(
            `Failed to get model ${modelPath} from package ${packageName} in environment ${environmentName}: ${error}`,
         );
      }
   }
}
