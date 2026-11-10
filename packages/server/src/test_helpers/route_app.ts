// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The production main app (`createMainApp`): its middleware and both REST
 * route tables, `/api/v0` and the legacy `/projects` alias, with controllers a
 * test supplies and no Console. Any controller method the test did not stub
 * throws, so a route that reaches something unexpected fails loudly instead
 * of answering from a fake.
 */

import type express from "express";
import { isQuerySlotMiddleware, QuerySlots } from "../query_concurrency";
import type { AppServices } from "../app_services";
import { createMainApp } from "../routes/main_app";
import { VersionRef } from "../service/identity";

export { API_PREFIX } from "../routes/main_app";

/**
 * A stubbed controller's arguments with each version the routes parsed (a
 * {@link VersionRef}, alone or as an options bag's `versionId`) read back as
 * the version it names: the string, or undefined for `latest`.
 */
export function namedVersions(args: unknown[]): unknown[] {
   return args.map((arg) => {
      if (arg instanceof VersionRef) return arg.requested();
      if (
         arg !== null &&
         typeof arg === "object" &&
         (arg as { versionId?: unknown }).versionId instanceof VersionRef
      ) {
         const bag = arg as { versionId: VersionRef };
         return { ...bag, versionId: bag.versionId.requested() };
      }
      return arg;
   });
}

type Stubs = { [K in keyof AppServices]?: Partial<AppServices[K]> };

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

function controllers(stubs: Stubs): AppServices {
   const names: (keyof AppServices)[] = [
      "environmentStore",
      "themeStore",
      "retrieval",
      "queryUseCase",
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
      "versionController",
      "querySlots",
   ];
   return Object.fromEntries(
      names.map((name) => [name, stubs[name] ?? unstubbed(name)]),
   ) as unknown as AppServices;
}

/** The main app as main_server.ts builds it, over the controllers given. */
export function buildRouteApp(stubs: Stubs = {}): express.Express {
   return createMainApp({
      controllers: controllers({
         // The deprecated-route controller is constructed at registration and
         // only stores this reference.
         environmentStore: {},
         // A pool of its own unless the spec shares one to hold slots in.
         querySlots: new QuerySlots(),
         ...stubs,
      }),
      dataAppRuntimePath: "/nonexistent/publisher.js",
   });
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
              gated: route.stack.some((layer) =>
                 isQuerySlotMiddleware(layer.handle),
              ),
           }))
         : [],
   );
}
