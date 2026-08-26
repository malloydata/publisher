// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import { runWithQueryTimeout } from "../query_timeout";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";

type ApiNotebook = components["schemas"]["Notebook"];
type ApiModel = components["schemas"]["Model"];
type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
export class ModelController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listModels(
      environmentName: string,
      packageName: string,
   ): Promise<ApiModel[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listModels();
   }

   public async listNotebooks(
      environmentName: string,
      packageName: string,
   ): Promise<ApiNotebook[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listNotebooks();
   }

   public async getModel(
      environmentName: string,
      packageName: string,
      modelPath: string,
   ): Promise<ApiCompiledModel> {
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const p = await environment.getPackage(packageName, false);
         const model = p.getModel(modelPath);
         if (!model) {
            throw new ModelNotFoundError(`${modelPath} does not exist`);
         }
         if (model.getType() === "notebook") {
            throw new ModelNotFoundError(`${modelPath} is a notebook`);
         }
         // The compiled view and the file's own text, together: the file is on
         // disk beside the package, and a client showing code next to the model
         // otherwise has no way to fetch it. The read stays on `getPackage`'s
         // lock-free fast path (see environment.ts: callers reachable from
         // `Model.getModel()` must not take the package lock): a single
         // `readFile` against a mid-swap tree sees the old file, the new file,
         // or ENOENT, and only the last needs handling — it withholds the text,
         // never the compiled model the spec marks it optional beside.
         const [compiled, sourceText] = await Promise.all([
            model.getModel(),
            p.getModelFileText(modelPath).catch((error: unknown) => {
               logger.warn("getModel: model source text unavailable", {
                  environmentName,
                  packageName,
                  modelPath,
                  error: error instanceof Error ? error.message : String(error),
               });
               return undefined;
            }),
         ]);
         return sourceText === undefined
            ? compiled
            : { ...compiled, sourceText };
      } catch (error) {
         // Re-throw ModelNotFoundError as-is
         if (error instanceof ModelNotFoundError) {
            throw error;
         }
         // Wrap other errors with more context
         throw new Error(
            `Failed to get model ${modelPath} from package ${packageName} in environment ${environmentName}: ${error}`,
         );
      }
   }

   public async getNotebook(
      environmentName: string,
      packageName: string,
      notebookPath: string,
   ): Promise<ApiRawNotebook> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return model.getNotebook();
   }

   public async executeNotebookCell(
      environmentName: string,
      packageName: string,
      notebookPath: string,
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Shed load before any disk / DB work; same rationale as
      // QueryController.getQuery — already-loaded packages bypass the
      // package-load admission gate.
      environment.assertCanAdmitQuery();
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return runWithQueryTimeout(
         (abortSignal) =>
            model.executeNotebookCell(
               cellIndex,
               filterParams,
               bypassFilters,
               givens,
               abortSignal,
               {
                  environment: environmentName,
                  // The package owns its manifest, so the least-specific
                  // author-declared layer is read here; the model knows only its
                  // own file and its package's NAME.
                  packageDeclaration: p.getDeclaredQueryMetadata(),
                  // The environment owns the connection configs, so the default
                  // and enforced layers are read here rather than from the model.
                  connectionMetadata: (connectionName) => {
                     try {
                        const connection =
                           environment.getApiConnection(connectionName);
                        return {
                           default: connection.queryMetadata,
                           enforced: connection.queryMetadataEnforced,
                        };
                     } catch (error) {
                        // Fails open, and says so: what an unreadable
                        // connection costs is the enforced layer.
                        logger.debug(
                           "No query-metadata layers for connection",
                           { connectionName, error },
                        );
                        return null;
                     }
                  },
               },
            ),
         getQueryTimeoutMs(),
      );
   }
}
