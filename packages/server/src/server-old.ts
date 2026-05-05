/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Legacy `/projects/...` route registration.
 *
 * The publisher's API was renamed from `projects` to `environments`. This module
 * registers the old `/projects/...` paths on the same Express app so existing
 * SDK clients (e.g. `@malloydata/db-publisher`) keep working without code
 * changes on their side.
 *
 * Implementation strategy:
 *   - Reuse the same controllers that `server.ts` wires up; only the URL
 *     surface changes.
 *   - Most response models (`Connection`, `Package`, `Model`, `Notebook`,
 *     `Database`, `Table`, `QueryResult`, etc.) have identical JSON wire
 *     format between old (`Project`) and new (`Environment`) specs, so they
 *     pass through unchanged.
 *   - The handful of payloads that DO have field-level renames are remapped:
 *       * GET /status                       — `environments` -> `projects`
 *       * Materialization responses         — `environmentId` -> `projectId`
 *
 *   - Watch-mode is intentionally not exposed under the legacy prefix; clients
 *     that need it should use the new `/environments/...` paths directly.
 */

import bodyParser from "body-parser";
import type { Express, Response } from "express";
import { ParsedQs } from "qs";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { ManifestController } from "./controller/manifest.controller";
import { MaterializationController } from "./controller/materialization.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import {
   BadRequestError,
   internalErrorToHttpError,
   NotImplementedError,
} from "./errors";
import { logger } from "./logger";
import { normalizeQueryArray } from "./server";
import { EnvironmentStore } from "./service/environment_store";

const LEGACY_API_PREFIX = "/api/v0";

/** Bag of controllers shared with the new server. */
export interface LegacyControllerSet {
   environmentStore: EnvironmentStore;
   connectionController: ConnectionController;
   modelController: ModelController;
   packageController: PackageController;
   databaseController: DatabaseController;
   queryController: QueryController;
   compileController: CompileController;
   materializationController: MaterializationController;
   manifestController: ManifestController;
}

// ─── response/body field mappers ───────────────────────────────────────────

function remapStatusResponse(status: any): any {
   if (!status || typeof status !== "object") return status;
   const out: Record<string, any> = { ...status };
   if ("environments" in out) {
      out.projects = out.environments;
      delete out.environments;
   }
   return out;
}

function remapMaterializationResponse(mat: any): any {
   if (!mat || typeof mat !== "object") return mat;
   if (Array.isArray(mat)) {
      return mat.map(remapMaterializationResponse);
   }
   const out: Record<string, any> = { ...mat };
   if ("environmentId" in out) {
      out.projectId = out.environmentId;
      delete out.environmentId;
   }
   return out;
}

const setVersionIdError = (res: Response) => {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError("Version IDs not implemented."),
   );
   res.status(status).json(json);
};

// ─── route registration ────────────────────────────────────────────────────

export function registerLegacyRoutes(
   app: Express,
   controllers: LegacyControllerSet,
) {
   const {
      environmentStore,
      connectionController,
      modelController,
      packageController,
      databaseController,
      queryController,
      compileController,
      materializationController,
      manifestController,
   } = controllers;

   // body-parser is already registered on the main app for `${API_PREFIX}/*`
   // paths via `app.use(bodyParser.json(...))`. The legacy routes share the
   // same `${API_PREFIX}` prefix so they inherit it automatically.
   void bodyParser; // keep the import; helper file reference for clarity

   // ── status ──────────────────────────────────────────────────────────────
   app.get(`${LEGACY_API_PREFIX}/status`, async (_req, res) => {
      try {
         const status = await environmentStore.getStatus();
         res.status(200).json(remapStatusResponse(status));
      } catch (error) {
         logger.error("Error getting status", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   });

   // ── projects (== environments) ──────────────────────────────────────────
   app.get(`${LEGACY_API_PREFIX}/projects`, async (_req, res) => {
      try {
         res.status(200).json(await environmentStore.listEnvironments());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   });

   app.post(`${LEGACY_API_PREFIX}/projects`, async (req, res) => {
      try {
         logger.info("Adding project", { body: req.body });
         const environment = await environmentStore.addEnvironment(req.body);
         res.status(200).json(await environment.serialize());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   });

   app.get(`${LEGACY_API_PREFIX}/projects/:projectName`, async (req, res) => {
      try {
         const environment = await environmentStore.getEnvironment(
            req.params.projectName,
            req.query.reload === "true",
         );
         res.status(200).json(await environment.serialize());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   });

   app.patch(`${LEGACY_API_PREFIX}/projects/:projectName`, async (req, res) => {
      try {
         const environment = await environmentStore.updateEnvironment(req.body);
         res.status(200).json(await environment.serialize());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   });

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName`,
      async (req, res) => {
         try {
            const environment = await environmentStore.deleteEnvironment(
               req.params.projectName,
            );
            res.status(200).json(await environment?.serialize());
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── connections ─────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.listConnections(
                  req.params.projectName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnection(
                  req.params.projectName,
                  req.params.connectionName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      async (req, res) => {
         try {
            const result = await connectionController.addConnection(
               req.params.projectName,
               req.params.connectionName,
               req.body,
            );
            res.status(201).json(result);
         } catch (error) {
            logger.error("Error creating connection", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.patch(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      async (req, res) => {
         try {
            const result = await connectionController.updateConnection(
               req.params.projectName,
               req.params.connectionName,
               req.body,
            );
            res.status(200).json(result);
         } catch (error) {
            logger.error("Error updating connection", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      async (req, res) => {
         try {
            const result = await connectionController.deleteConnection(
               req.params.projectName,
               req.params.connectionName,
            );
            res.status(200).json(result);
         } catch (error) {
            logger.error("Error deleting connection", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // /connections/test is org-level (no projectName) and unchanged between
   // old and new specs — it's already registered on the main app.

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.listSchemas(
                  req.params.projectName,
                  req.params.connectionName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables`,
      async (req, res) => {
         try {
            const results = await connectionController.listTables(
               req.params.projectName,
               req.params.connectionName,
               req.params.schemaName,
               normalizeQueryArray(req.query.tableNames),
            );
            res.status(200).json(results);
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
      async (req, res) => {
         try {
            const results = await connectionController.getTable(
               req.params.projectName,
               req.params.connectionName,
               req.params.schemaName,
               req.params.tablePath,
            );
            res.status(200).json(results);
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // Per-package connection routes (duckdb context)
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.listSchemas(
                  req.params.projectName,
                  req.params.connectionName,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.listTables(
                  req.params.projectName,
                  req.params.connectionName,
                  req.params.schemaName,
                  normalizeQueryArray(req.query.tableNames),
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getTable(
                  req.params.projectName,
                  req.params.connectionName,
                  req.params.schemaName,
                  req.params.tablePath,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // sqlSource (deprecated GET + supported POST), per-project + per-package
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionSqlSource(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionSqlSource(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlSource`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionSqlSource(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlSource`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionSqlSource(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // queryData (deprecated GET) + sqlQuery (supported POST), per-project +
   // per-package
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/queryData`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionQueryData(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
                  req.query.options as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/queryData`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionQueryData(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
                  req.query.options as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlQuery`,
      async (req, res) => {
         try {
            let options: string | ParsedQs | (string | ParsedQs)[] | undefined;
            if (req.body?.options) {
               options = req.body.options;
            } else {
               options = req.query.options;
            }
            res.status(200).json(
               await connectionController.getConnectionQueryData(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
                  options as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlQuery`,
      async (req, res) => {
         try {
            let options: string | ParsedQs | (string | ParsedQs)[] | undefined;
            if (req.body?.options) {
               options = req.body.options;
            } else {
               options = req.query.options;
            }
            res.status(200).json(
               await connectionController.getConnectionQueryData(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
                  options as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // temporaryTable (deprecated GET) + sqlTemporaryTable (supported POST)
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/temporaryTable`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionTemporaryTable(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/temporaryTable`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionTemporaryTable(
                  req.params.projectName,
                  req.params.connectionName,
                  req.query.sqlStatement as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlTemporaryTable`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionTemporaryTable(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlTemporaryTable`,
      async (req, res) => {
         try {
            res.status(200).json(
               await connectionController.getConnectionTemporaryTable(
                  req.params.projectName,
                  req.params.connectionName,
                  req.body.sqlStatement as string,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── packages ────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            res.status(200).json(
               await packageController.listPackages(req.params.projectName),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages`,
      async (req, res) => {
         try {
            const autoLoadManifest = req.query.autoLoadManifest === "true";
            const _package = await packageController.addPackage(
               req.params.projectName,
               req.body,
               { autoLoadManifest },
            );
            res.status(200).json(_package?.getPackageMetadata());
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            res.status(200).json(
               await packageController.getPackage(
                  req.params.projectName,
                  req.params.packageName,
                  req.query.reload === "true",
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.patch(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      async (req, res) => {
         try {
            res.status(200).json(
               await packageController.updatePackage(
                  req.params.projectName,
                  req.params.packageName,
                  req.body,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      async (req, res) => {
         try {
            res.status(200).json(
               await packageController.deletePackage(
                  req.params.projectName,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── models ──────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            res.status(200).json(
               await modelController.listModels(
                  req.params.projectName,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            const modelPath = (req.params as Record<string, string>)["0"];
            res.status(200).json(
               await modelController.getModel(
                  req.params.projectName,
                  req.params.packageName,
                  modelPath,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?/query`,
      async (req, res) => {
         if (req.body.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            const modelPath = (req.params as Record<string, string>)["0"];
            res.status(200).json(
               await queryController.getQuery(
                  req.params.projectName,
                  req.params.packageName,
                  modelPath,
                  req.body.sourceName as string,
                  req.body.queryName as string,
                  req.body.query as string,
                  req.body.compactJson === true,
                  (req.body.filterParams ?? req.body.sourceFilters) as
                     | Record<string, string | string[]>
                     | undefined,
                  req.body.bypassFilters === true ? true : undefined,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/:modelName/compile`,
      async (req, res) => {
         try {
            const result = await compileController.compile(
               req.params.projectName,
               req.params.packageName,
               req.params.modelName,
               req.body.source,
               req.body.includeSql === true,
            );
            res.status(200).json(result);
         } catch (error) {
            logger.error("Compilation error", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── notebooks ───────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            res.status(200).json(
               await modelController.listNotebooks(
                  req.params.projectName,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // Cell execution route comes BEFORE the general getNotebook wildcard
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*/cells/:cellIndex`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            const cellIndex = parseInt(req.params.cellIndex, 10);
            if (isNaN(cellIndex)) {
               res.status(400).json({ error: "Invalid cell index" });
               return;
            }
            const notebookPath = (req.params as Record<string, string>)["0"];
            let filterParams: Record<string, string | string[]> | undefined;
            if (typeof req.query.filter_params === "string") {
               try {
                  filterParams = JSON.parse(req.query.filter_params);
               } catch {
                  res.status(400).json({
                     error: "Invalid filter_params: must be valid JSON",
                  });
                  return;
               }
            }
            const bypassFilters =
               req.query.bypass_filters === "true" ? true : undefined;
            res.status(200).json(
               await modelController.executeNotebookCell(
                  req.params.projectName,
                  req.params.packageName,
                  notebookPath,
                  cellIndex,
                  filterParams,
                  bypassFilters,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*?`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            const notebookPath = (req.params as Record<string, string>)["0"];
            res.status(200).json(
               await modelController.getNotebook(
                  req.params.projectName,
                  req.params.packageName,
                  notebookPath,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── databases ───────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/databases`,
      async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         try {
            res.status(200).json(
               await databaseController.listDatabases(
                  req.params.projectName,
                  req.params.packageName,
               ),
            );
         } catch (error) {
            logger.error(error);
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── materializations ────────────────────────────────────────────────────
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations`,
      async (req, res) => {
         try {
            const build = await materializationController.createMaterialization(
               req.params.projectName,
               req.params.packageName,
               req.body || {},
            );
            res.status(201).json(remapMaterializationResponse(build));
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations`,
      async (req, res) => {
         try {
            const limit = req.query.limit
               ? parseInt(req.query.limit as string, 10)
               : undefined;
            const offset = req.query.offset
               ? parseInt(req.query.offset as string, 10)
               : undefined;
            const builds = await materializationController.listMaterializations(
               req.params.projectName,
               req.params.packageName,
               { limit, offset },
            );
            res.status(200).json(remapMaterializationResponse(builds));
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      async (req, res) => {
         try {
            const build = await materializationController.getMaterialization(
               req.params.projectName,
               req.params.packageName,
               req.params.materializationId,
            );
            res.status(200).json(remapMaterializationResponse(build));
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/teardown`,
      async (req, res) => {
         try {
            const result = await materializationController.teardownPackage(
               req.params.projectName,
               req.params.packageName,
               req.body || {},
            );
            res.status(200).json(result);
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      async (req, res) => {
         try {
            const action = req.query.action;
            if (action === "start") {
               const build =
                  await materializationController.startMaterialization(
                     req.params.projectName,
                     req.params.packageName,
                     req.params.materializationId,
                  );
               res.status(202).json(remapMaterializationResponse(build));
            } else if (action === "stop") {
               const build =
                  await materializationController.stopMaterialization(
                     req.params.projectName,
                     req.params.packageName,
                     req.params.materializationId,
                  );
               res.status(200).json(remapMaterializationResponse(build));
            } else {
               throw new BadRequestError(
                  `Unsupported action '${String(action ?? "")}'. Expected 'start' or 'stop'.`,
               );
            }
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      async (req, res) => {
         try {
            await materializationController.deleteMaterialization(
               req.params.projectName,
               req.params.packageName,
               req.params.materializationId,
            );
            res.status(204).send();
         } catch (error) {
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   // ── manifest ────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/manifest`,
      async (req, res) => {
         try {
            const manifest = await manifestController.getManifest(
               req.params.projectName,
               req.params.packageName,
            );
            res.status(200).json(manifest);
         } catch (error) {
            logger.error("Get manifest error", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/manifest`,
      async (req, res) => {
         try {
            const action = req.query.action;
            if (action === "reload") {
               const manifest = await manifestController.reloadManifest(
                  req.params.projectName,
                  req.params.packageName,
               );
               res.status(200).json(manifest);
            } else {
               throw new BadRequestError(
                  `Unsupported action '${String(action ?? "")}'. Expected 'reload'.`,
               );
            }
         } catch (error) {
            logger.error("Manifest action error", { error });
            const { json, status } = internalErrorToHttpError(error as Error);
            res.status(status).json(json);
         }
      },
   );

   logger.info(
      "Legacy /projects/* routes registered for backwards compatibility",
   );
}
