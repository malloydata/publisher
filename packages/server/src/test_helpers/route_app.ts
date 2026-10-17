// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The real REST route table — `/api/v0` and the legacy `/projects` alias — on
 * a bare Express app, with controllers a test supplies. Any controller method
 * the test did not stub throws, so a route that reaches something unexpected
 * fails loudly instead of answering from a fake.
 */

import express from "express";
import { queryConcurrencyMiddleware } from "../query_concurrency";
import { registerApiRoutes, type ApiControllers } from "../routes/api";
import { registerDataAppListRoute } from "../routes/data_apps";
import { unhandledErrorMiddleware } from "../routes/json_route";
import { registerLegacyRoutes } from "../server-old";

export const API_PREFIX = "/api/v0";

type Stubs = { [K in keyof ApiControllers]?: Partial<ApiControllers[K]> };

/** A controller whose every method, when called, names what the test forgot. */
function unstubbed(name: string): object {
   return new Proxy(
      {},
      {
         get(_target, property) {
            if (property === "then") return undefined;
            return () => {
               throw new Error(`${name}.${String(property)} was not stubbed`);
            };
         },
      },
   );
}

function controllers(stubs: Stubs): ApiControllers {
   const names: (keyof ApiControllers)[] = [
      "environmentStore",
      "themeStore",
      "themeController",
      "watchModeController",
      "connectionController",
      "packageController",
      "modelController",
      "skillController",
      "reportController",
      "databaseController",
      "queryController",
      "compileController",
      "materializationController",
      "storageDestinationController",
   ];
   return Object.fromEntries(
      names.map((name) => [name, stubs[name] ?? unstubbed(name)]),
   ) as unknown as ApiControllers;
}

/** Both route tables, mounted the way server.ts mounts them. */
export function buildRouteApp(stubs: Stubs = {}): express.Express {
   const app = express();
   app.use(express.json());
   const c = controllers({
      // The deprecated-route controller is constructed at registration and
      // only stores this reference.
      environmentStore: {},
      ...stubs,
   });
   registerDataAppListRoute(app, API_PREFIX, c.environmentStore);
   registerApiRoutes(app, API_PREFIX, c);
   registerLegacyRoutes(app, c);
   app.use(unhandledErrorMiddleware);
   return app;
}

export interface RegisteredRoute {
   method: string;
   path: string;
   gated: boolean;
}

/** Every route the app registers, and whether the concurrency gate guards it. */
export function registeredRoutes(app: express.Express): RegisteredRoute[] {
   type Layer = {
      route?: {
         path: string;
         methods: Record<string, boolean>;
         stack: { handle: unknown }[];
      };
   };
   const stack = (app as unknown as { _router: { stack: Layer[] } })._router
      .stack;
   return stack.flatMap(({ route }) =>
      route
         ? Object.keys(route.methods).map((method) => ({
              method: method.toUpperCase(),
              path: route.path,
              gated: route.stack.some(
                 (layer) => layer.handle === queryConcurrencyMiddleware,
              ),
           }))
         : [],
   );
}
