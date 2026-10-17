// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `/api/v0` route table. Every JSON route goes through {@link apiRoute} or
 * {@link jsonRoute}, which map a thrown error to its status and log it once:
 * a 4xx at `warn`, a 5xx at `error`. Registration order matters where paths
 * overlap (the `models/*?` routes), so keep it.
 */

import type { GivenValue } from "@malloydata/malloy";
import type express from "express";
import { readBypassAuthorize } from "../authorize_bypass_header";
import type { CompileController } from "../controller/compile.controller";
import type { ConnectionController } from "../controller/connection.controller";
import type { DatabaseController } from "../controller/database.controller";
import type { MaterializationController } from "../controller/materialization.controller";
import type { ModelController } from "../controller/model.controller";
import type { PackageController } from "../controller/package.controller";
import type { QueryController } from "../controller/query.controller";
import type { ReportController } from "../controller/report.controller";
import type { SkillController } from "../controller/skill.controller";
import type { StorageDestinationController } from "../controller/storage_destination.controller";
import type { ThemeController } from "../controller/theme.controller";
import type { WatchModeController } from "../controller/watch-mode.controller";
import { BadRequestError, ServiceUnavailableError } from "../errors";
import { setFilterDeprecationHeaders } from "../filter_deprecation";
import { logger, redactSensitive } from "../logger";
import { assertSafePackageName } from "../path_safety";
import { queryConcurrency } from "../query_concurrency";
import {
   normalizeQueryArray,
   parseNonNegativeIntParam,
} from "../query_param_utils";
import { timeReportRead } from "../report_metrics";
import { booleanParamOr400, setCollectionReloadError } from "../route_params";
import { processStorageDestinationsOrThrow } from "../service/connection_config";
import type { EnvironmentStore } from "../service/environment_store";
import type { ThemeStore } from "../service/theme_store";
import { registerDeprecatedRoutes } from "./deprecated";
import {
   apiRoute,
   jsonRoute,
   logRefusal,
   sendRouteError,
   setVersionIdError,
   wildcardParam,
} from "./json_route";
import { reportWriteRoute } from "./report_write";

export interface ApiControllers {
   environmentStore: EnvironmentStore;
   themeStore: ThemeStore;
   themeController: ThemeController;
   watchModeController: WatchModeController;
   connectionController: ConnectionController;
   packageController: PackageController;
   modelController: ModelController;
   skillController: SkillController;
   reportController: ReportController;
   databaseController: DatabaseController;
   queryController: QueryController;
   compileController: CompileController;
   materializationController: MaterializationController;
   storageDestinationController: StorageDestinationController;
}

// Cap concurrent live-reload SSE connections so the endpoint can't be used to
// exhaust server sockets/memory with unbounded long-lived streams. Generous,
// since legitimate use is one stream per open dashboard tab.
const MAX_SSE_CONNECTIONS = 1000;

export function registerApiRoutes(
   app: express.Express,
   apiPrefix: string,
   c: ApiControllers,
): void {
   const env = `${apiPrefix}/environments/:environmentName`;
   const pkg = `${env}/packages/:packageName`;

   registerStatusAndThemeRoutes(app, apiPrefix, c);
   registerWatchModeRoutes(app, apiPrefix, pkg, c);
   registerEnvironmentRoutes(app, apiPrefix, env, c);
   registerConnectionRoutes(app, apiPrefix, env, pkg, c);
   registerPackageRoutes(app, apiPrefix, env, pkg, c);
   registerQueryRoutes(app, pkg, c);
   registerMaterializationRoutes(app, env, pkg, c);
}

function registerStatusAndThemeRoutes(
   app: express.Express,
   apiPrefix: string,
   { environmentStore, themeStore, themeController }: ApiControllers,
): void {
   app.get(
      `${apiPrefix}/status`,
      apiRoute(async (req) => {
         // Packages still loading for the first time are listed only on request:
         // a listed package has always meant one that can serve here, and only a
         // caller that reads `Package.status` can tell the two apart.
         const status = await environmentStore.getStatus({
            includeLoading: req.query.includeLoading === "true",
         });
         // The theme rides along so the SDK reads both in one round trip on app
         // boot; `{}` means "no overrides yet". It is cosmetic, so before storage
         // initializes this reports no overrides rather than 500 an endpoint the
         // control plane polls for serving state (themeStore.get() throws until
         // storage is ready). GET /theme answers 503 during that window instead.
         const theme = environmentStore.storageManager.isInitialized()
            ? await themeStore.get()
            : undefined;
         return { ...status, theme: theme ?? {} };
      }),
   );

   app.get(
      `${apiPrefix}/theme`,
      apiRoute(() => {
         if (!environmentStore.storageManager.isInitialized()) {
            // 503, not 200 with an empty theme, so the Theme Editor's load stays
            // in an error state and never adopts {} as the saved baseline, which
            // would let a later edit auto-save {} over the persisted theme.
            throw new ServiceUnavailableError(
               "Theme storage is still initializing. Retry shortly.",
            );
         }
         return themeController.getTheme();
      }),
   );
   app.put(
      `${apiPrefix}/theme`,
      apiRoute((req) => themeController.putTheme(req.body)),
   );
   app.delete(
      `${apiPrefix}/theme`,
      apiRoute(() => themeController.resetTheme()),
   );
}

function registerWatchModeRoutes(
   app: express.Express,
   apiPrefix: string,
   pkg: string,
   { environmentStore, watchModeController }: ApiControllers,
): void {
   app.get(
      `${apiPrefix}/watch-mode/status`,
      watchModeController.getWatchStatus,
   );
   app.post(`${apiPrefix}/watch-mode/start`, watchModeController.startWatching);
   app.post(`${apiPrefix}/watch-mode/stop`, watchModeController.stopWatchMode);

   // Live-reload Server-Sent Events for in-package HTML dashboards. This does
   // not start watch mode (that is `--watch-env` or POST watch-mode/start), so
   // an arbitrary fetch cannot mutate global watch state; it reports whether
   // watch mode is active for the env in a `mode` event and, if so, fans out
   // change events. Names are checked with the shared `assertSafePackageName`
   // before any lookup, so `%2e%2e` never reaches the EnvironmentStore.
   let sseConnectionCount = 0;
   app.get(`${pkg}/events`, async (req: express.Request, res) => {
      const envName = req.params.environmentName;
      const pkgName = req.params.packageName;
      try {
         assertSafePackageName(envName);
         assertSafePackageName(pkgName);
         const environment = await environmentStore.getEnvironment(
            envName,
            false,
         );
         await environment.getPackage(pkgName, false); // 404 if missing
      } catch (error) {
         sendRouteError(res, error as Error, logRefusal(req, error as Error));
         return;
      }

      if (sseConnectionCount >= MAX_SSE_CONNECTIONS) {
         res.status(503).json({
            code: 503,
            message: "Too many live-reload connections; try again shortly.",
         });
         return;
      }
      sseConnectionCount++;

      res.set({
         "content-type": "text/event-stream",
         "cache-control": "no-cache",
         connection: "keep-alive",
         // Disable proxy/CDN buffering so events flush immediately.
         "x-accel-buffering": "no",
      });
      res.flushHeaders();

      const watching = watchModeController.isWatching(envName);
      res.write("event: hello\ndata: connected\n\n");
      res.write(`event: mode\ndata: ${watching ? "enabled" : "disabled"}\n\n`);

      const key = `${envName}/${pkgName}`;
      const send = () => {
         res.write("event: changed\ndata: changed\n\n");
      };
      watchModeController.events.on(key, send);
      // Keep the connection alive through idle proxies.
      const heartbeat = setInterval(() => {
         res.write(": heartbeat\n\n");
      }, 25000);
      // "close" covers both clean and abrupt disconnects on Node >= 20.
      req.on("close", () => {
         clearInterval(heartbeat);
         watchModeController.events.off(key, send);
         sseConnectionCount--;
      });
   });
}

function registerEnvironmentRoutes(
   app: express.Express,
   apiPrefix: string,
   env: string,
   { environmentStore }: ApiControllers,
): void {
   app.get(
      `${apiPrefix}/environments`,
      apiRoute((req, res) => {
         if (req.query.reload !== undefined) {
            setCollectionReloadError(
               res,
               `${apiPrefix}/environments/{environmentName}`,
            );
            return;
         }
         return environmentStore.listEnvironments();
      }),
   );

   app.post(
      `${apiPrefix}/environments`,
      apiRoute(async (req) => {
         // Redacted like every body-bearing log line: the body carries
         // connection and storage-destination configs.
         logger.info("Adding environment", { body: redactSensitive(req.body) });
         // Strict here, where the author is waiting, and lenient in
         // `addEnvironment`, which boot and restore also call: a destination
         // this body fails to describe must not come back as a 200 that quietly
         // omits it. There is no stored list on create, so a bare validation is
         // enough.
         processStorageDestinationsOrThrow(req.body?.storageDestinations ?? []);
         const environment = await environmentStore.addEnvironment(req.body);
         return environment.serialize();
      }),
   );

   app.get(
      env,
      apiRoute(async (req, res) => {
         const reload = booleanParamOr400(req, res, "reload");
         if (reload === undefined) return;
         const environment = await environmentStore.getEnvironment(
            req.params.environmentName,
            reload,
         );
         return environment.serialize();
      }),
   );

   app.patch(
      env,
      apiRoute(async (req) => {
         const environment = await environmentStore.updateEnvironment(req.body);
         return environment.serialize();
      }),
   );

   app.delete(
      env,
      apiRoute(async (req) => {
         const environment = await environmentStore.deleteEnvironment(
            req.params.environmentName,
         );
         return environment?.serialize();
      }),
   );
}

function registerConnectionRoutes(
   app: express.Express,
   apiPrefix: string,
   env: string,
   pkg: string,
   { connectionController }: ApiControllers,
): void {
   const conn = `${env}/connections/:connectionName`;
   const pkgConn = `${pkg}/connections/:connectionName`;

   app.get(
      `${env}/connections`,
      apiRoute((req) =>
         connectionController.listConnections(req.params.environmentName),
      ),
   );
   app.get(
      conn,
      apiRoute((req) =>
         connectionController.getConnection(
            req.params.environmentName,
            req.params.connectionName,
         ),
      ),
   );
   app.post(
      conn,
      apiRoute(
         (req) =>
            connectionController.addConnection(
               req.params.environmentName,
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
            req.params.environmentName,
            req.params.connectionName,
            req.body,
         ),
      ),
   );
   app.delete(
      conn,
      apiRoute((req) =>
         connectionController.deleteConnection(
            req.params.environmentName,
            req.params.connectionName,
         ),
      ),
   );
   app.post(
      `${apiPrefix}/connections/test`,
      apiRoute((req) =>
         connectionController.testConnectionConfiguration(req.body),
      ),
   );

   // Schema browsing, environment-wide and per package. `duckdb` is
   // per-package; other names fall through to the environment's connections
   // via the package's MalloyConfig wrapper.
   for (const [prefix, scoped] of [
      [conn, false],
      [pkgConn, true],
   ] as const) {
      const packageName = (req: express.Request) =>
         scoped ? req.params.packageName : undefined;
      app.get(
         `${prefix}/schemas`,
         apiRoute((req) =>
            connectionController.listSchemas(
               req.params.environmentName,
               req.params.connectionName,
               packageName(req),
            ),
         ),
      );
      app.get(
         `${prefix}/schemas/:schemaName/tables`,
         apiRoute((req) =>
            connectionController.listTables(
               req.params.environmentName,
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
               req.params.environmentName,
               req.params.connectionName,
               req.params.schemaName,
               req.params.tablePath,
               packageName(req),
            ),
         ),
      );
   }

   // SQL against a connection runs live database work (a DESCRIBE, a query, a
   // temp table), so each is admission-controlled like a Malloy query.
   const bothScopes = (
      suffix: string,
      handler: (
         req: express.Request,
         packageName: string | undefined,
      ) => Promise<unknown>,
   ) => {
      app.post(
         `${conn}${suffix}`,
         queryConcurrency(),
         apiRoute((req) => handler(req, undefined)),
      );
      app.post(
         `${pkgConn}${suffix}`,
         queryConcurrency(),
         apiRoute((req) => handler(req, req.params.packageName)),
      );
   };
   bothScopes("/sqlSource", (req, packageName) =>
      connectionController.getConnectionSqlSource(
         req.params.environmentName,
         req.params.connectionName,
         req.body.sqlStatement as string,
         packageName,
      ),
   );
   bothScopes("/sqlQuery", (req, packageName) =>
      connectionController.getConnectionQueryData(
         req.params.environmentName,
         req.params.connectionName,
         req.body.sqlStatement as string,
         req.body?.options as string,
         packageName,
         {
            queryMetadata: req.body?.queryMetadata,
            queryClass: req.body?.queryClass,
         },
      ),
   );
   bothScopes("/sqlTemporaryTable", (req, packageName) =>
      connectionController.getConnectionTemporaryTable(
         req.params.environmentName,
         req.params.connectionName,
         req.body.sqlStatement as string,
         packageName,
      ),
   );
}

function registerPackageRoutes(
   app: express.Express,
   apiPrefix: string,
   env: string,
   pkg: string,
   c: ApiControllers,
): void {
   const {
      packageController,
      modelController,
      skillController,
      reportController,
      environmentStore,
   } = c;

   app.get(
      `${env}/packages`,
      jsonRoute((req, res) => {
         if (req.query.reload !== undefined) {
            setCollectionReloadError(
               res,
               `${apiPrefix}/environments/${req.params.environmentName}/packages/{packageName}`,
            );
            return;
         }
         return packageController.listPackages(req.params.environmentName);
      }),
   );
   app.post(
      `${env}/packages`,
      apiRoute(async (req) => {
         const added = await packageController.addPackage(
            req.params.environmentName,
            req.body,
         );
         return added?.getPackageMetadata();
      }),
   );
   app.get(
      pkg,
      jsonRoute((req, res) => {
         const reload = booleanParamOr400(req, res, "reload");
         if (reload === undefined) return;
         return packageController.getPackage(
            req.params.environmentName,
            req.params.packageName,
            reload,
         );
      }),
   );
   app.patch(
      pkg,
      apiRoute((req) =>
         packageController.updatePackage(
            req.params.environmentName,
            req.params.packageName,
            req.body,
         ),
      ),
   );
   app.delete(
      pkg,
      apiRoute((req) =>
         packageController.deletePackage(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );

   app.get(
      `${pkg}/models`,
      jsonRoute((req, res) => {
         const includeHiddenFilesAndSources = booleanParamOr400(
            req,
            res,
            "includeHiddenFilesAndSources",
         );
         if (includeHiddenFilesAndSources === undefined) return;
         return modelController.listModels(
            req.params.environmentName,
            req.params.packageName,
            { includeHiddenFilesAndSources },
         );
      }),
   );
   app.get(
      `${pkg}/models/*?`,
      jsonRoute((req, res) => {
         const includeHiddenFilesAndSources = booleanParamOr400(
            req,
            res,
            "includeHiddenFilesAndSources",
         );
         if (includeHiddenFilesAndSources === undefined) return;
         return modelController.getModel(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            { includeHiddenFilesAndSources },
         );
      }),
   );

   // A skill or agent-skill name can carry a slash (`<skill>/<stem>` for a
   // reference file), so it is a wildcard rather than a named parameter.
   // Express has already decoded the wildcard; decoding again throws on a bare %.
   app.get(
      `${pkg}/skills`,
      apiRoute((req) =>
         skillController.listSkills(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );
   app.get(
      `${pkg}/skills/*`,
      apiRoute((req) =>
         skillController.getSkill(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req) ?? "",
         ),
      ),
   );
   app.get(
      `${pkg}/agents`,
      apiRoute((req) =>
         skillController.listAgents(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );
   app.get(
      `${pkg}/agents/:agentName`,
      apiRoute((req, res) => {
         const skills = req.query.skills ?? "full";
         if (skills !== "full" && skills !== "summary") {
            res.status(400).json({
               message: `'skills' must be 'full' or 'summary', got ${JSON.stringify(skills)}.`,
            });
            return;
         }
         return skillController.getAgent(
            req.params.environmentName,
            req.params.packageName,
            req.params.agentName,
            skills,
         );
      }),
   );
   app.get(
      `${pkg}/agents/:agentName/skills/*`,
      apiRoute((req) =>
         skillController.getAgentSkillFile(
            req.params.environmentName,
            req.params.packageName,
            req.params.agentName,
            wildcardParam(req) ?? "",
         ),
      ),
   );

   app.put(
      `${pkg}/models/*?`,
      // A report save compiles the submitted text and writes it under the
      // package lock across a full reload (and a rollback reload on failure),
      // strictly more work than one /compile. Ungated it would also convoy:
      // the save holds the package mutex without a slot, so slot-holding
      // compiles on that package pile up behind it.
      queryConcurrency(),
      reportWriteRoute(reportController),
   );
   app.get(
      `${pkg}/reports`,
      timeReportRead("list"),
      jsonRoute((req) =>
         reportController.listReports(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );
   app.get(
      `${pkg}/reports/*`,
      timeReportRead("get"),
      jsonRoute((req) =>
         reportController.getReport(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
         ),
      ),
   );

   registerDeprecatedRoutes(app, apiPrefix, environmentStore);
}

function registerQueryRoutes(
   app: express.Express,
   pkg: string,
   { queryController, databaseController, compileController }: ApiControllers,
): void {
   app.post(`${pkg}/models/*?/query`, queryConcurrency(), async (req, res) => {
      if (req.body.versionId) {
         setVersionIdError(res);
         return;
      }
      const includeHiddenFilesAndSources = booleanParamOr400(
         req,
         res,
         "includeHiddenFilesAndSources",
      );
      if (includeHiddenFilesAndSources === undefined) return;

      // A client that goes away (a superseded dashboard tile, a closed tab)
      // cancels its query: the concurrency slot is released on `close`, so
      // the query must not keep running past it.
      const disconnected = new AbortController();
      res.on("close", () => {
         if (!res.writableFinished)
            disconnected.abort(new Error("client disconnected"));
      });

      try {
         const filterParams = (req.body.filterParams ??
            req.body.sourceFilters) as
            | Record<string, string | string[]>
            | undefined;
         const bypassFilters =
            req.body.bypassFilters === true ? true : undefined;
         const result = await queryController.getQuery(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            req.body.sourceName as string,
            req.body.queryName as string,
            req.body.query as string,
            req.body.compactJson === true,
            filterParams,
            bypassFilters,
            req.body.givens as Record<string, GivenValue> | undefined,
            {
               queryMetadata: req.body?.queryMetadata,
               queryClass: req.body?.queryClass,
               versionId: req.body?.versionId as string | undefined,
            },
            // Disables the author's `#(authorize)` gates. From a HEADER,
            // never the body, and nothing in Publisher bounds who may send
            // it: the deployment must strip it at its edge. See
            // authorize_bypass_header.ts and
            // docs/authorize-bypass-deployment.md.
            readBypassAuthorize(req),
            includeHiddenFilesAndSources,
            disconnected.signal,
         );
         setFilterDeprecationHeaders(res, { filterParams, bypassFilters });
         res.status(200).json(result);
      } catch (error) {
         // Nobody is waiting for the answer: not worth a log line.
         if (disconnected.signal.aborted) return;
         sendRouteError(res, error as Error, logRefusal(req, error as Error));
      }
   });

   app.get(
      `${pkg}/databases`,
      jsonRoute((req) =>
         databaseController.listDatabases(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );

   app.post(
      `${pkg}/models/*?/compile`,
      // Compile runs real Malloy compilation (and resolves source schemas
      // against the connection), so it is admission-controlled like a query.
      queryConcurrency(),
      apiRoute((req) =>
         compileController.compile(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            req.body.source,
            req.body.includeSql === true,
            req.body.givens as Record<string, GivenValue> | undefined,
            // An invalid scope is rejected by compileSource with a 400 naming
            // the valid set, never silently consumed.
            req.body.scope ?? "append",
         ),
      ),
   );
}

function registerMaterializationRoutes(
   app: express.Express,
   env: string,
   pkg: string,
   { materializationController, storageDestinationController }: ApiControllers,
): void {
   // Package-scoped: a materialization is a run of one package's persist
   // sources and cannot exist without a package.
   const materializations = `${pkg}/materializations`;
   const materialization = `${materializations}/:materializationId`;

   app.post(
      materializations,
      apiRoute(
         (req) =>
            materializationController.createMaterialization(
               req.params.environmentName,
               req.params.packageName,
               req.body || {},
            ),
         { status: 201 },
      ),
   );
   app.get(
      materializations,
      apiRoute((req) =>
         materializationController.listMaterializations(
            req.params.environmentName,
            req.params.packageName,
            {
               limit: parseNonNegativeIntParam(req.query.limit),
               offset: parseNonNegativeIntParam(req.query.offset),
            },
         ),
      ),
   );
   app.get(
      materialization,
      apiRoute((req) =>
         materializationController.getMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
         ),
      ),
   );
   app.post(
      materialization,
      apiRoute((req) => {
         const action = req.query.action;
         if (action !== "stop") {
            throw new BadRequestError(
               `Unsupported action '${String(action ?? "")}'. Expected 'stop'.`,
            );
         }
         return materializationController.stopMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
         );
      }),
   );
   app.delete(
      materialization,
      apiRoute(async (req, res) => {
         const dropTables = booleanParamOr400(req, res, "dropTables");
         if (dropTables === undefined) return;
         await materializationController.deleteMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
            { dropTables },
         );
         res.status(204).send();
      }),
   );

   // Table lifecycle inside an environment's storage destinations, for an
   // orchestrator that assigns its own physical table names. Destinations are
   // never reachable through the connection routes, so these are the only way
   // to drop, enumerate, or reclaim the storage behind a table in one.
   const destination = `${env}/storageDestinations/:destinationName`;
   app.get(
      `${destination}/schemas/:schemaName/tables`,
      apiRoute((req) =>
         storageDestinationController.listTables(
            req.params.environmentName,
            req.params.destinationName,
            req.params.schemaName,
         ),
      ),
   );
   app.delete(
      `${destination}/schemas/:schemaName/tables/:tableName`,
      apiRoute(async (req, res) => {
         await storageDestinationController.dropTable(
            req.params.environmentName,
            req.params.destinationName,
            req.params.schemaName,
            req.params.tableName,
         );
         res.status(204).send();
      }),
   );
   app.post(
      `${destination}/fileCleanups`,
      apiRoute(
         (req) =>
            storageDestinationController.createFileCleanup(
               req.params.environmentName,
               req.params.destinationName,
               req.body ?? {},
            ),
         { status: 201 },
      ),
   );
}
