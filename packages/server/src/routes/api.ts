// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `/api/v0` route table. Every JSON route goes through {@link apiRoute},
 * which maps a thrown error to its status and logs it once:
 * a 4xx at `warn`, a 5xx at `error`. Registration order matters where paths
 * overlap (the `models/*?` routes), so keep it.
 */

import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import type { GivenValue } from "@malloydata/malloy";
import type express from "express";
import { readBypassAuthorize } from "../authorize_bypass_header";
import type { AppServices } from "../app_services";
import { accessIdentityOf } from "../service/access_record";
import { BadRequestError, ServiceUnavailableError } from "../errors";
import { setFilterDeprecationHeaders } from "../filter_deprecation";
import { logger, redactSensitive } from "../logger";
import {
   normalizeQueryArray,
   parseNonNegativeIntParam,
} from "../query_param_utils";
import { timeReportRead } from "../report_metrics";
import {
   assertListingTakesNoVersionId,
   booleanParam,
   optionalBooleanParam,
   queryVersionRefOf,
   assertNoCollectionReload,
   versionRefOf,
} from "../route_params";
import { processStorageDestinationsOrThrow } from "../service/connection_config";
import { registerDeprecatedRoutes } from "./deprecated";
import {
   apiRoute,
   legacyErrorBody,
   logRefusal,
   recordGateRefusal,
   sendRouteError,
   wildcardParam,
   markDeprecated,
} from "./json_route";
import { registerStorageDestinationRoutes } from "./storage_destinations";
import { reportWriteRoute } from "./report_write";
import { registerVersionRoutes } from "./versions";
import {
   parseEnvironmentName,
   parsePackageRef,
   VersionRef,
} from "../service/identity";

// Caps long-lived SSE streams so they cannot exhaust sockets or memory.
// Generous: legitimate use is one stream per open dashboard tab.
const MAX_SSE_CONNECTIONS = 1000;

export function registerApiRoutes(
   app: express.Express,
   apiPrefix: string,
   c: AppServices,
): void {
   const env = `${apiPrefix}/environments/:environmentName`;
   const pkg = `${env}/packages/:packageName`;

   registerStatusAndThemeRoutes(app, apiPrefix, c);
   registerWatchModeRoutes(app, apiPrefix, pkg, c);
   registerEnvironmentRoutes(app, apiPrefix, env, c);
   registerConnectionRoutes(app, apiPrefix, env, pkg, c);
   registerPackageRoutes(app, apiPrefix, env, pkg, c);
   registerQueryRoutes(app, pkg, c);
   registerMaterializationRoutes(app, pkg, c);
   // Deprecated: the admin API's catalog resources replace them.
   registerStorageDestinationRoutes(
      app,
      apiPrefix,
      c.storageDestinationController,
   );
}

function registerStatusAndThemeRoutes(
   app: express.Express,
   apiPrefix: string,
   { environmentStore, themeStore, themeController }: AppServices,
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
         // The theme rides along to save the SDK a round trip; `{}` means no
         // overrides. Before storage is ready (themeStore.get() throws) it
         // reports `{}` rather than 500 an endpoint the control plane polls;
         // GET /theme answers 503 in that window instead.
         const theme = themeStore.isReady()
            ? await themeStore.get()
            : undefined;
         return { ...status, theme: theme ?? {} };
      }),
   );

   app.get(
      `${apiPrefix}/theme`,
      apiRoute(() => {
         if (!themeStore.isReady()) {
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
   { environmentStore, watchModeController }: AppServices,
): void {
   // Deprecated here: the admin API serves watch mode, an operator's control.
   // These keep their `{ error }` failure body and empty success body.
   const watchRoute = (run: (req: express.Request) => Promise<unknown>) =>
      apiRoute(run, { errorBody: legacyErrorBody });
   app.get(
      `${apiPrefix}/watch-mode/status`,
      markDeprecated,
      watchRoute(async () => watchModeController.current()),
   );
   app.post(
      `${apiPrefix}/watch-mode/start`,
      markDeprecated,
      watchRoute(async (req) => {
         await watchModeController.watch(req.body.environmentName ?? "");
      }),
   );
   app.post(
      `${apiPrefix}/watch-mode/stop`,
      markDeprecated,
      watchRoute(() => watchModeController.unwatch()),
   );

   // Live-reload SSE for in-package HTML dashboards. It never starts watch
   // mode, so an arbitrary fetch cannot mutate global watch state; it reports
   // the env's watch state in a `mode` event and fans out change events. Names
   // are validated before any lookup, so `%2e%2e` never reaches the store.
   let sseConnectionCount = 0;
   app.get(`${pkg}/events`, async (req: express.Request, res) => {
      const envName = req.params.environmentName;
      const pkgName = req.params.packageName;
      try {
         const ref = parsePackageRef({
            environmentName: envName,
            packageName: pkgName,
            versionId: req.query.versionId,
         });
         const environment = await environmentStore.getEnvironment(
            ref.environment,
            false,
         );
         // 404 if missing; 404/410 for a version it does not serve.
         await environment.getPackage(ref.package, false, {
            versionId: ref.version,
         });
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
      // "close" covers clean and abrupt disconnects on Node >= 20.
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
   { environmentStore }: AppServices,
): void {
   app.get(
      `${apiPrefix}/environments`,
      apiRoute((req) => {
         assertNoCollectionReload(
            req,
            `${apiPrefix}/environments/{environmentName}`,
         );
         return environmentStore.listEnvironments();
      }),
   );

   app.post(
      `${apiPrefix}/environments`,
      apiRoute(async (req) => {
         // Redacted like every body-bearing log line: the body carries
         // connection and storage-destination configs.
         logger.info("Adding environment", { body: redactSensitive(req.body) });
         // Strict here, where the author is waiting (`addEnvironment` is
         // lenient for boot and restore): a destination this body fails to
         // describe must not come back as a 200 that quietly omits it.
         processStorageDestinationsOrThrow(req.body?.storageDestinations ?? []);
         const environment = await environmentStore.addEnvironment(req.body);
         return environment.serialize();
      }),
   );

   app.get(
      env,
      apiRoute(async (req, res) => {
         const reload = booleanParam(req, "reload");
         if (reload) {
            // Deprecated: re-reads the config file's environment block. No
            // client calls it, v1 has no environment reload, and removal waits
            // on this count reading zero (#1540).
            res.set("Deprecation", "true");
            recordDeprecatedUsage("environment_reload");
         }
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(req.params.environmentName),
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
   { connectionController, querySlots }: AppServices,
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
      // A package-scoped route reads the version its request names.
      const versionId = (req: express.Request) =>
         scoped ? versionRefOf(req) : VersionRef.LATEST;
      app.get(
         `${prefix}/schemas`,
         apiRoute((req) =>
            connectionController.listSchemas(
               req.params.environmentName,
               req.params.connectionName,
               packageName(req),
               versionId(req),
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
               versionId(req),
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
               versionId(req),
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
         versionId: VersionRef,
      ) => Promise<unknown>,
   ) => {
      app.post(
         `${conn}${suffix}`,
         querySlots.middleware,
         apiRoute((req) => handler(req, undefined, VersionRef.LATEST)),
      );
      app.post(
         `${pkgConn}${suffix}`,
         querySlots.middleware,
         apiRoute((req) =>
            handler(req, req.params.packageName, versionRefOf(req)),
         ),
      );
   };
   bothScopes("/sqlSource", (req, packageName, versionId) =>
      connectionController.getConnectionSqlSource(
         req.params.environmentName,
         req.params.connectionName,
         req.body.sqlStatement as string,
         packageName,
         versionId,
      ),
   );
   bothScopes("/sqlQuery", (req, packageName, versionId) =>
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
         versionId,
         accessIdentityOf(req.headers),
      ),
   );
   bothScopes("/sqlTemporaryTable", (req, packageName, versionId) =>
      connectionController.getConnectionTemporaryTable(
         req.params.environmentName,
         req.params.connectionName,
         req.body.sqlStatement as string,
         packageName,
         versionId,
         accessIdentityOf(req.headers),
      ),
   );
}

/**
 * The `givens` query parameter, a JSON object the trusted tier sets for the
 * caller, so a model read can omit sources the caller cannot enter (see
 * service/discovery_lock.ts). `undefined` when absent.
 */
function givensParam(
   req: express.Request,
): Record<string, GivenValue> | undefined {
   const raw = req.query.givens;
   if (raw === undefined) return undefined;
   if (typeof raw === "string") {
      try {
         const parsed = JSON.parse(raw) as unknown;
         if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            return parsed as Record<string, GivenValue>;
         }
      } catch {
         // fall through to the 400
      }
   }
   throw new BadRequestError("givens must be one JSON object");
}

function registerPackageRoutes(
   app: express.Express,
   apiPrefix: string,
   env: string,
   pkg: string,
   c: AppServices,
): void {
   const {
      packageController,
      modelController,
      skillController,
      reportController,
      environmentStore,
      versionController,
      querySlots,
   } = c;

   app.get(
      `${env}/packages`,
      apiRoute((req) => {
         assertListingTakesNoVersionId(req.query.versionId);
         assertNoCollectionReload(
            req,
            `${apiPrefix}/environments/${req.params.environmentName}/packages/{packageName}`,
         );
         return packageController.listPackages(req.params.environmentName);
      }),
   );
   app.post(
      `${env}/packages`,
      apiRoute(async (req) => {
         const dryRun = booleanParam(req, "dryRun");
         if (dryRun) {
            return packageController.dryRunPackage(
               req.params.environmentName,
               req.body,
            );
         }
         const added = await packageController.addPackage(
            req.params.environmentName,
            req.body,
         );
         // The same metadata as before for a package with no versions; a
         // published version also carries the package's current `latest`.
         if (!added) return undefined;
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(req.params.environmentName),
            false,
         );
         return environment.describePackage(added);
      }),
   );
   // A package's published versions, their lifecycle and its `latest`.
   registerVersionRoutes(app, pkg, versionController);
   app.get(
      pkg,
      apiRoute((req) => {
         const reload = booleanParam(req, "reload");
         return packageController.getPackage(
            req.params.environmentName,
            req.params.packageName,
            reload,
            versionRefOf(req),
         );
      }),
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
      apiRoute((req) => {
         const includeHiddenFilesAndSources = booleanParam(
            req,
            "includeHiddenFilesAndSources",
         );
         return modelController.listModels(
            req.params.environmentName,
            req.params.packageName,
            { includeHiddenFilesAndSources, versionId: versionRefOf(req) },
         );
      }),
   );
   app.get(
      `${pkg}/models/*?`,
      apiRoute((req) => {
         const includeHiddenFilesAndSources = booleanParam(
            req,
            "includeHiddenFilesAndSources",
         );
         const givens = givensParam(req);
         return modelController.getModel(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            {
               includeHiddenFilesAndSources,
               versionId: versionRefOf(req),
               ...(givens ? { givens } : {}),
            },
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
            versionRefOf(req),
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
            versionRefOf(req),
         ),
      ),
   );
   // A package's sub-resources, each read for the version `versionId` names.
   const packageReads: [
      string,
      (
         environmentName: string,
         packageName: string,
         versionId: VersionRef,
      ) => Promise<unknown>,
   ][] = [
      [
         "status",
         (e, p, v) => packageController.getPackageServingState(e, p, v),
      ],
      [
         "build-plan",
         (e, p, v) => packageController.getPackageBuildPlan(e, p, v),
      ],
      [
         "classification",
         (e, p, v) => packageController.getPackageClassification(e, p, v),
      ],
      [
         "embedding-index",
         (e, p, v) => packageController.getPackageEmbeddingIndex(e, p, v),
      ],
   ];
   app.get(
      `${pkg}/entities`,
      apiRoute((req) => {
         const includeHiddenFilesAndSources = booleanParam(
            req,
            "includeHiddenFilesAndSources",
         );
         return packageController.getPackageEntities(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
            includeHiddenFilesAndSources,
         );
      }),
   );
   for (const [resource, read] of packageReads) {
      app.get(
         `${pkg}/${resource}`,
         apiRoute((req) =>
            read(
               req.params.environmentName,
               req.params.packageName,
               versionRefOf(req),
            ),
         ),
      );
   }
   app.get(
      `${pkg}/agents`,
      apiRoute((req) =>
         skillController.listAgents(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
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
            versionRefOf(req),
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
            versionRefOf(req),
         ),
      ),
   );

   app.put(
      `${pkg}/models/*?`,
      // A save compiles and reloads under the package lock, more work than
      // one /compile. Ungated it would also convoy: slot-holding compiles on
      // the package would pile up behind a save holding the mutex.
      querySlots.middleware,
      reportWriteRoute(reportController),
   );
   app.get(
      `${pkg}/reports`,
      timeReportRead("list"),
      apiRoute((req) =>
         reportController.listReports(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
         ),
      ),
   );
   app.get(
      `${pkg}/reports/*`,
      timeReportRead("get"),
      apiRoute((req) =>
         reportController.getReport(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            versionRefOf(req),
         ),
      ),
   );

   registerDeprecatedRoutes(app, apiPrefix, environmentStore, querySlots);
}

function registerQueryRoutes(
   app: express.Express,
   pkg: string,
   {
      queryController,
      databaseController,
      compileController,
      querySlots,
   }: AppServices,
): void {
   const recordRefusal = (req: express.Request, error: Error) =>
      queryController.recordRefusal(
         req.params.environmentName,
         req.params.packageName,
         wildcardParam(req),
         req.body ?? {},
         accessIdentityOf(req.headers),
         error,
      );
   app.post(
      `${pkg}/models/*?/query`,
      querySlots.middleware,
      async (req: express.Request, res: express.Response) => {
         let includeHiddenFilesAndSources: boolean;
         try {
            includeHiddenFilesAndSources =
               optionalBooleanParam(req, "includeHiddenFilesAndSources") ??
               false;
         } catch (error) {
            await recordRefusal(req, error as Error);
            sendRouteError(
               res,
               error as Error,
               logRefusal(req, error as Error),
            );
            return;
         }

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
            const result = await queryController.getQuery({
               environmentName: req.params.environmentName,
               packageName: req.params.packageName,
               modelPath: wildcardParam(req),
               sourceName: req.body.sourceName,
               queryName: req.body.queryName,
               query: req.body.query,
               compactJson: req.body.compactJson === true,
               filterParams,
               bypassFilters,
               givens: req.body.givens,
               queryMetadata: req.body?.queryMetadata,
               queryClass: req.body?.queryClass,
               versionId: queryVersionRefOf(req),
               // Disables the author's `#(authorize)` gates. From a header, never
               // the body, and nothing here bounds who may send it: the
               // deployment must strip it at its edge. See
               // docs/authorize-bypass-deployment.md.
               bypassAuthorize: readBypassAuthorize(req),
               includeHiddenFilesAndSources,
               clientSignal: disconnected.signal,
               // For the access record: who the gateway says is asking, and the
               // trace this request belongs to. See docs/access-record.md.
               identity: accessIdentityOf(req.headers),
            });
            setFilterDeprecationHeaders(res, { filterParams, bypassFilters });
            res.status(200).json(result);
         } catch (error) {
            // Nobody is waiting for the answer: not worth a log line.
            if (disconnected.signal.aborted) return;
            sendRouteError(
               res,
               error as Error,
               logRefusal(req, error as Error),
            );
         }
      },
      recordGateRefusal(recordRefusal),
   );

   app.get(
      `${pkg}/databases`,
      apiRoute((req) =>
         databaseController.listDatabases(
            req.params.environmentName,
            req.params.packageName,
            versionRefOf(req),
         ),
      ),
   );

   app.post(
      `${pkg}/models/*?/compile`,
      // Compile runs real Malloy compilation (and resolves source schemas
      // against the connection), so it is admission-controlled like a query.
      querySlots.middleware,
      apiRoute((req) =>
         compileController.compile(
            req.params.environmentName,
            req.params.packageName,
            wildcardParam(req),
            req.body.source,
            req.body.includeSql === true,
            req.body.givens as Record<string, GivenValue> | undefined,
            // compileSource rejects an invalid scope with a 400.
            req.body.scope ?? "append",
            versionRefOf(req),
            req.body.files,
         ),
      ),
   );
}

function registerMaterializationRoutes(
   app: express.Express,
   pkg: string,
   { materializationController }: AppServices,
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
               versionId: versionRefOf(req),
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
            versionRefOf(req),
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
            versionRefOf(req),
         );
      }),
   );
   app.delete(
      materialization,
      apiRoute(async (req, res) => {
         const dropTables = booleanParam(req, "dropTables");
         await materializationController.deleteMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
            { dropTables, versionId: versionRefOf(req) },
         );
         res.status(204).send();
      }),
   );
}
