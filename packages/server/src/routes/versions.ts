// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type express from "express";
import type { VersionController } from "../controller/version.controller";
import { assertSafePackageName } from "../path_safety";
import { apiRoute } from "./json_route";

/**
 * The versions routes, registered on the app's route table like every other
 * `/api/v0` route: each answers through {@link apiRoute}, so a refusal is
 * logged once at `warn` and a failure once at `error`.
 */
export function registerVersionRoutes(
   app: express.Express,
   pkg: string,
   controller: VersionController,
): void {
   const route = (run: (req: express.Request) => Promise<unknown>) =>
      apiRoute((req) => {
         assertSafePackageName(req.params.packageName);
         return run(req);
      });

   app.get(
      `${pkg}/versions`,
      route((req) =>
         controller.listVersions(
            req.params.environmentName,
            req.params.packageName,
         ),
      ),
   );
   app.get(
      `${pkg}/versions/:version`,
      route((req) =>
         controller.getVersion(
            req.params.environmentName,
            req.params.packageName,
            req.params.version,
         ),
      ),
   );
   app.patch(
      `${pkg}/versions/:version`,
      route((req) =>
         controller.updateVersion(
            req.params.environmentName,
            req.params.packageName,
            req.params.version,
            req.body,
         ),
      ),
   );
   app.delete(
      `${pkg}/versions/:version`,
      route((req) =>
         controller.deleteVersion(
            req.params.environmentName,
            req.params.packageName,
            req.params.version,
         ),
      ),
   );
   app.put(
      `${pkg}/versions/:version/manifest`,
      route((req) =>
         controller.setManifest(
            req.params.environmentName,
            req.params.packageName,
            req.params.version,
            req.body,
         ),
      ),
   );
   app.put(
      `${pkg}/latest`,
      route((req) =>
         controller.setLatest(
            req.params.environmentName,
            req.params.packageName,
            req.body,
         ),
      ),
   );
}
