// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Legacy `/projects/...` aliases of the `/environments/...` API, kept for SDK
 * clients that predate the rename (e.g. `@malloydata/db-publisher`). Removable
 * once those clients have migrated.
 *
 * - Reuses the controllers `main_server.ts` wires up; only the URL surface
 *   differs. The routes that declared `versionId` read it as the
 *   `/environments` routes do: omitted means `latest`.
 * - Most payloads are wire-identical between the two specs; materialization
 *   responses are remapped `environmentId` -> `projectId`.
 * - Watch-mode is not exposed here. `/status` is shared, so legacy clients get
 *   the `environments`-keyed payload.
 */

import type { Express, Request } from "express";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { MaterializationController } from "./controller/materialization.controller";
import { LegacyReportController } from "./controller/legacy_report.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { BadRequestError } from "./errors";
import { logger, redactSensitive } from "./logger";
import { accessIdentityOf } from "./service/access_record";
import { notebookCellHandlers } from "./routes/deprecated";
import {
   apiRoute,
   recordGateRefusal,
   wildcardParam,
} from "./routes/json_route";
import type { QuerySlots } from "./query_concurrency";
import { normalizeQueryArray } from "./query_param_utils";
import {
   assertListingTakesNoVersionId,
   booleanParam,
   queryVersionRefOf,
   assertNoCollectionReload,
   versionRefOf,
} from "./route_params";
import { processStorageDestinationsOrThrow } from "./service/connection_config";
import type { EnvironmentAdmin } from "./service/environment_ports";
import { parseEnvironmentName } from "./service/identity";

const LEGACY_API_PREFIX = "/api/v0";

/** Bag of controllers shared with the new server. */
export interface LegacyControllerSet {
   environmentStore: EnvironmentAdmin;
   connectionController: ConnectionController;
   modelController: ModelController;
   packageController: PackageController;
   databaseController: DatabaseController;
   queryController: QueryController;
   compileController: CompileController;
   materializationController: MaterializationController;
   /** The process's one query-slot pool, shared with the new routes. */
   querySlots: QuerySlots;
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
      querySlots,
   } = controllers;
   const legacyReportController = new LegacyReportController(environmentStore);
   // ── projects (== environments) ──────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects`,
      apiRoute(async (req) => {
         assertNoCollectionReload(
            req,
            `${LEGACY_API_PREFIX}/projects/{projectName}`,
         );
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
      apiRoute(async (req) => {
         const reload = booleanParam(req, "reload");
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(req.params.projectName),
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
   const conn = `${LEGACY_API_PREFIX}/projects/:projectName/connections/:connectionName`;
   const pkgConn = `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/connections/:connectionName`;

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/connections`,
      apiRoute((req) =>
         connectionController.listConnections(req.params.projectName),
      ),
   );
   app.get(
      conn,
      apiRoute((req) =>
         connectionController.getConnection(
            req.params.projectName,
            req.params.connectionName,
         ),
      ),
   );
   app.post(
      conn,
      apiRoute(
         (req) =>
            connectionController.addConnection(
               req.params.projectName,
               req.params.connectionName,
               req.body,
            ),
         { status: 201 },
      ),
   );
   app.patch(
      conn,
      apiRoute((req) =>
         connectionController.updateConnection(
            req.params.projectName,
            req.params.connectionName,
            req.body,
         ),
      ),
   );
   app.delete(
      conn,
      apiRoute((req) =>
         connectionController.deleteConnection(
            req.params.projectName,
            req.params.connectionName,
         ),
      ),
   );
   // /connections/test has no project segment and is served by the main app.

   // Each route project-wide and per package (the package's duckdb context).
   // Unlike the modern routes these read no versionId. The SQL routes run live
   // database work, so they are admission-controlled like a query; timeouts
   // and row/byte caps are enforced by the shared controllers.
   for (const [prefix, scoped] of [
      [conn, false],
      [pkgConn, true],
   ] as const) {
      const packageName = (req: Request) =>
         scoped ? req.params.packageName : undefined;
      app.get(
         `${prefix}/schemas`,
         apiRoute((req) =>
            connectionController.listSchemas(
               req.params.projectName,
               req.params.connectionName,
               packageName(req),
            ),
         ),
      );
      app.get(
         `${prefix}/schemas/:schemaName/tables`,
         apiRoute((req) =>
            connectionController.listTables(
               req.params.projectName,
               req.params.connectionName,
               req.params.schemaName,
               normalizeQueryArray(req.query.tableNames),
               packageName(req),
            ),
         ),
      );
      app.get(
         `${prefix}/schemas/:schemaName/tables/:tablePath`,
         apiRoute((req) =>
            connectionController.getTable(
               req.params.projectName,
               req.params.connectionName,
               req.params.schemaName,
               req.params.tablePath,
               packageName(req),
            ),
         ),
      );
      app.post(
         `${prefix}/sqlSource`,
         querySlots.middleware,
         apiRoute((req) =>
            connectionController.getConnectionSqlSource(
               req.params.projectName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               packageName(req),
            ),
         ),
      );
      // queryData (GET) was removed from the modern surface.
      app.get(
         `${prefix}/queryData`,
         querySlots.middleware,
         apiRoute((req) =>
            connectionController.getConnectionQueryData(
               req.params.projectName,
               req.params.connectionName,
               req.query.sqlStatement as string,
               req.query.options as string,
               packageName(req),
               undefined,
               undefined,
               accessIdentityOf(req.headers),
            ),
         ),
      );
      app.post(
         `${prefix}/sqlQuery`,
         querySlots.middleware,
         apiRoute((req) =>
            connectionController.getConnectionQueryData(
               req.params.projectName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               (req.body?.options || req.query.options) as string,
               packageName(req),
               undefined,
               undefined,
               accessIdentityOf(req.headers),
            ),
         ),
      );
      app.post(
         `${prefix}/sqlTemporaryTable`,
         querySlots.middleware,
         apiRoute((req) =>
            connectionController.getConnectionTemporaryTable(
               req.params.projectName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               packageName(req),
               undefined,
               accessIdentityOf(req.headers),
            ),
         ),
      );
   }

   // ── packages ────────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages`,
      apiRoute(async (req) => {
         assertListingTakesNoVersionId(req.query.versionId);
         assertNoCollectionReload(
            req,
            `${LEGACY_API_PREFIX}/projects/${req.params.projectName}/packages/{packageName}`,
         );
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
      apiRoute(async (req) => {
         const reload = booleanParam(req, "reload");
         return packageController.getPackage(
            req.params.projectName,
            req.params.packageName,
            reload,
            versionRefOf(req),
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
      apiRoute(async (req) => {
         return modelController.listModels(
            req.params.projectName,
            req.params.packageName,
            { versionId: versionRefOf(req) },
         );
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?`,
      apiRoute(async (req) => {
         const modelPath = wildcardParam(req);
         return modelController.getModel(
            req.params.projectName,
            req.params.packageName,
            modelPath,
            { versionId: versionRefOf(req) },
         );
      }),
   );

   // Accepts no authorize bypass, deliberately: it passes no `givens` either,
   // and every route that can disable a gate is one more to audit. Pinned by
   // authorize_bypass_wiring.
   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/*?/query`,
      querySlots.middleware,
      apiRoute(async (req) => {
         return queryController.getQuery({
            environmentName: req.params.projectName,
            packageName: req.params.packageName,
            modelPath: wildcardParam(req),
            sourceName: req.body.sourceName,
            queryName: req.body.queryName,
            query: req.body.query,
            compactJson: req.body.compactJson === true,
            filterParams: req.body.filterParams ?? req.body.sourceFilters,
            bypassFilters: req.body.bypassFilters === true ? true : undefined,
            versionId: queryVersionRefOf(req),
            identity: accessIdentityOf(req.headers),
         });
      }),
      recordGateRefusal((req, error) =>
         queryController.recordRefusal(
            req.params.projectName,
            req.params.packageName,
            wildcardParam(req),
            req.body ?? {},
            accessIdentityOf(req.headers),
            error,
         ),
      ),
   );

   app.post(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/models/:modelName/compile`,
      // Gated like the modern compile route.
      querySlots.middleware,
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
      apiRoute(async (req) => {
         return legacyReportController.listNotebooks(
            req.params.projectName,
            req.params.packageName,
            versionRefOf(req),
         );
      }),
   );

   // Before the general getNotebook wildcard, so it does not swallow `/cells/{i}`.
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*/cells/:cellIndex`,
      ...notebookCellHandlers(legacyReportController, {
         environmentParam: "projectName",
         legacy: true,
         querySlots,
      }),
   );

   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/notebooks/*?`,
      apiRoute(async (req) => {
         const notebookPath = wildcardParam(req);
         return legacyReportController.getNotebook(
            req.params.projectName,
            req.params.packageName,
            notebookPath,
            versionRefOf(req),
         );
      }),
   );

   // ── databases ───────────────────────────────────────────────────────────
   app.get(
      `${LEGACY_API_PREFIX}/projects/:projectName/packages/:packageName/databases`,
      apiRoute(async (req) => {
         return databaseController.listDatabases(
            req.params.projectName,
            req.params.packageName,
            versionRefOf(req),
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
         const dropTables = booleanParam(req, "dropTables");
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
