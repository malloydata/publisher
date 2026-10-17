// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Legacy `/projects/...` aliases of the `/environments/...` API, kept for SDK
 * clients that predate the rename (e.g. `@malloydata/db-publisher`). Removable
 * once those clients have migrated.
 *
 * - Reuses the controllers `server.ts` wires up; only the URL surface differs.
 * - Most payloads are wire-identical between the two specs; materialization
 *   responses are remapped `environmentId` -> `projectId`.
 * - Watch-mode is not exposed here. `/status` is shared, so legacy clients get
 *   the `environments`-keyed payload.
 */

import bodyParser from "body-parser";
import type { Express, Response } from "express";
import { ParsedQs } from "qs";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { MaterializationController } from "./controller/materialization.controller";
import { LegacyReportController } from "./controller/legacy_report.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import {
   BadRequestError,
   internalErrorToHttpError,
   NotImplementedError,
} from "./errors";
import { logger, redactSensitive } from "./logger";
import { apiRoute } from "./routes/json_route";
import { queryConcurrency } from "./query_concurrency";
import { normalizeQueryArray } from "./query_param_utils";
import {
   booleanParamOr400,
   optionalBooleanParamOr400,
   setCollectionReloadError,
} from "./route_params";
import { processStorageDestinationsOrThrow } from "./service/connection_config";
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
}

// ─── response/body field mappers ───────────────────────────────────────────

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
   } = controllers;
   const legacyReportController = new LegacyReportController(environmentStore);

   // body-parser is inherited from the main app.
   void bodyParser; // keep the import; helper file reference for clarity

   // ── projects (== environments) ──────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects`,
      apiRoute(async (req, res) => {
         if (req.query.reload !== undefined) {
            setCollectionReloadError(
               res,
               `${LEGACY_API_PREFIX}/projects/{projectName}`,
            );
            return;
         }
         return environmentStore.listEnvironments();
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects`,
      apiRoute(async (req) => {
         // Redacted for the same reason as the `POST /environments` twin: the
         // body can carry connection and storage-destination configs.
         logger.info("Adding project", { body: redactSensitive(req.body) });
         // Gated like its twin. An alias is still a create, so a destination the
         // server cannot read must not come back as a 200 that quietly omits it.
         processStorageDestinationsOrThrow(req.body?.storageDestinations ?? []);
         const environment = await environmentStore.addEnvironment(req.body);
         return environment.serialize();
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName`,
      apiRoute(async (req, res) => {
         const reload = booleanParamOr400(req, res, "reload");
         if (reload === undefined) {
            return;
         }
         const environment = await environmentStore.getEnvironment(
            req.params.projectName,
            reload,
         );
         return environment.serialize();
      }),
   );

   app.patch(
      `${LEGACY_API_PREFIX}/projects/:projectName`,
      apiRoute(async (req) => {
         const environment = await environmentStore.updateEnvironment(req.body);
         return environment.serialize();
      }),
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName`,
      apiRoute(async (req) => {
         const environment = await environmentStore.deleteEnvironment(
            req.params.projectName,
         );
         return environment?.serialize();
      }),
   );

   // ── connections ─────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections`,
      apiRoute(async (req) => {
         return connectionController.listConnections(req.params.projectName);
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      apiRoute(async (req) => {
         return connectionController.getConnection(
            req.params.projectName,
            req.params.connectionName,
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      apiRoute(
         async (req) => {
            return connectionController.addConnection(
               req.params.projectName,
               req.params.connectionName,
               req.body,
            );
         },
         { status: 201 },
      ),
   );

   app.patch(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      apiRoute(async (req) => {
         return connectionController.updateConnection(
            req.params.projectName,
            req.params.connectionName,
            req.body,
         );
      }),
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`,
      apiRoute(async (req) => {
         return connectionController.deleteConnection(
            req.params.projectName,
            req.params.connectionName,
         );
      }),
   );

   // /connections/test has no project segment and is served by the main app.

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas`,
      apiRoute(async (req) => {
         return connectionController.listSchemas(
            req.params.projectName,
            req.params.connectionName,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables`,
      apiRoute(async (req) => {
         return connectionController.listTables(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
            normalizeQueryArray(req.query.tableNames),
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
      apiRoute(async (req) => {
         return connectionController.getTable(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
            req.params.tablePath,
         );
      }),
   );

   // Per-package connection routes (duckdb context)
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas`,
      apiRoute(async (req) => {
         return connectionController.listSchemas(
            req.params.projectName,
            req.params.connectionName,
            req.params.packageName,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables`,
      apiRoute(async (req) => {
         return connectionController.listTables(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
            normalizeQueryArray(req.query.tableNames),
            req.params.packageName,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
      apiRoute(async (req) => {
         return connectionController.getTable(
            req.params.projectName,
            req.params.connectionName,
            req.params.schemaName,
            req.params.tablePath,
            req.params.packageName,
         );
      }),
   );

   // sqlSource runs a live DB introspection, so it is gated like the modern
   // route; otherwise the legacy surface would be an unbounded bypass.
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlSource`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionSqlSource(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlSource`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionSqlSource(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
            req.params.packageName,
         );
      }),
   );

   // queryData (GET, removed from the modern surface) and sqlQuery (POST).
   // queryConcurrency() applies the same per-pod cap as the modern routes;
   // timeouts and row/byte caps are enforced by the shared controllers.
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/queryData`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionQueryData(
            req.params.projectName,
            req.params.connectionName,
            req.query.sqlStatement as string,
            req.query.options as string,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/queryData`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionQueryData(
            req.params.projectName,
            req.params.connectionName,
            req.query.sqlStatement as string,
            req.query.options as string,
            req.params.packageName,
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlQuery`,
      queryConcurrency(),
      apiRoute(async (req) => {
         let options: string | ParsedQs | (string | ParsedQs)[] | undefined;
         if (req.body?.options) {
            options = req.body.options;
         } else {
            options = req.query.options;
         }
         return connectionController.getConnectionQueryData(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
            options as string,
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlQuery`,
      queryConcurrency(),
      apiRoute(async (req) => {
         let options: string | ParsedQs | (string | ParsedQs)[] | undefined;
         if (req.body?.options) {
            options = req.body.options;
         } else {
            options = req.query.options;
         }
         return connectionController.getConnectionQueryData(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
            options as string,
            req.params.packageName,
         );
      }),
   );

   // sqlTemporaryTable (POST), per-project + per-package
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName/sqlTemporaryTable`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionTemporaryTable(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName/sqlTemporaryTable`,
      queryConcurrency(),
      apiRoute(async (req) => {
         return connectionController.getConnectionTemporaryTable(
            req.params.projectName,
            req.params.connectionName,
            req.body.sqlStatement as string,
            req.params.packageName,
         );
      }),
   );

   // ── packages ────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         if (req.query.reload !== undefined) {
            setCollectionReloadError(
               res,
               `${LEGACY_API_PREFIX}/projects/${req.params.projectName}/packages/{packageName}`,
            );
            return;
         }
         return packageController.listPackages(req.params.projectName);
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages`,
      apiRoute(async (req) => {
         const _package = await packageController.addPackage(
            req.params.projectName,
            req.body,
         );
         return _package?.getPackageMetadata();
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         const reload = booleanParamOr400(req, res, "reload");
         if (reload === undefined) {
            return;
         }
         return packageController.getPackage(
            req.params.projectName,
            req.params.packageName,
            reload,
         );
      }),
   );

   app.patch(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      apiRoute(async (req) => {
         return packageController.updatePackage(
            req.params.projectName,
            req.params.packageName,
            req.body,
         );
      }),
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName`,
      apiRoute(async (req) => {
         return packageController.deletePackage(
            req.params.projectName,
            req.params.packageName,
         );
      }),
   );

   // ── models ──────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         return modelController.listModels(
            req.params.projectName,
            req.params.packageName,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         const modelPath = (req.params as Record<string, string>)["0"];
         return modelController.getModel(
            req.params.projectName,
            req.params.packageName,
            modelPath,
         );
      }),
   );

   // Accepts no authorize bypass, deliberately: it passes no `givens` either,
   // and every route that can disable a gate is one more to audit. Pinned by
   // authorize_bypass_wiring.
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?/query`,
      queryConcurrency(),
      apiRoute(async (req, res) => {
         if (req.body.versionId) {
            setVersionIdError(res);
            return;
         }
         const modelPath = (req.params as Record<string, string>)["0"];
         return queryController.getQuery(
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
         );
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/:modelName/compile`,
      // Gated like the modern compile route.
      queryConcurrency(),
      apiRoute(async (req) => {
         return compileController.compile(
            req.params.projectName,
            req.params.packageName,
            req.params.modelName,
            req.body.source,
            req.body.includeSql === true,
         );
      }),
   );

   // ── notebooks ───────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         return legacyReportController.listNotebooks(
            req.params.projectName,
            req.params.packageName,
         );
      }),
   );

   // Cell execution route comes BEFORE the general getNotebook wildcard
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*/cells/:cellIndex`,
      queryConcurrency(),
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
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
         // Absence must stay distinguishable from an explicit `false`:
         // the Deprecation header fires on `bypassFilters !== undefined`.
         const bypass = optionalBooleanParamOr400(req, res, "bypass_filters");
         if (!bypass.ok) {
            return;
         }
         const bypassFilters = bypass.value;
         return legacyReportController.executeNotebookCell(
            req.params.projectName,
            req.params.packageName,
            notebookPath,
            cellIndex,
            filterParams,
            bypassFilters,
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*?`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         const notebookPath = (req.params as Record<string, string>)["0"];
         return legacyReportController.getNotebook(
            req.params.projectName,
            req.params.packageName,
            notebookPath,
         );
      }),
   );

   // ── databases ───────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/databases`,
      apiRoute(async (req, res) => {
         if (req.query.versionId) {
            setVersionIdError(res);
            return;
         }
         return databaseController.listDatabases(
            req.params.projectName,
            req.params.packageName,
         );
      }),
   );

   // ── materializations ────────────────────────────────────────────────────
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations`,
      apiRoute(
         async (req) => {
            const build = await materializationController.createMaterialization(
               req.params.projectName,
               req.params.packageName,
               req.body || {},
            );
            return remapMaterializationResponse(build);
         },
         { status: 201 },
      ),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations`,
      apiRoute(async (req) => {
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
         return remapMaterializationResponse(builds);
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      apiRoute(async (req) => {
         const build = await materializationController.getMaterialization(
            req.params.projectName,
            req.params.packageName,
            req.params.materializationId,
         );
         return remapMaterializationResponse(build);
      }),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      apiRoute(async (req) => {
         const action = req.query.action;
         if (action !== "stop") {
            throw new BadRequestError(
               `Unsupported action '${String(action ?? "")}'. Expected 'stop'.`,
            );
         }
         return remapMaterializationResponse(
            await materializationController.stopMaterialization(
               req.params.projectName,
               req.params.packageName,
               req.params.materializationId,
            ),
         );
      }),
   );

   app.delete(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/materializations/:materializationId`,
      apiRoute(async (req, res) => {
         const dropTables = booleanParamOr400(req, res, "dropTables");
         if (dropTables === undefined) return;
         await materializationController.deleteMaterialization(
            req.params.projectName,
            req.params.packageName,
            req.params.materializationId,
            { dropTables },
         );
         res.status(204).send();
      }),
   );

   logger.info(
      "Legacy /projects/* routes registered for backwards compatibility",
   );
}
