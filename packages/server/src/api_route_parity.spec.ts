// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { createAdminApp } from "./routes/admin";
import { buildRouteApp, registeredRoutes } from "./test_helpers/route_app";

/**
 * Every route is written twice: as an Express registration and as a path in
 * api-doc.yaml, which every generated client is built from. Nothing else keeps
 * the two in step, and a drift is silent: each side stays internally
 * consistent, every client compiles, and the shipped SDK requests a path the
 * server does not serve. (The Console hides one such drift by design: it
 * swallows a 404 from the data-apps listing.) The legacy `/projects` alias is
 * not in the spec and is excluded.
 */

const REPO_ROOT = resolve(import.meta.dir, "../../..");

/** `{name}`, `:name`, `*` and `*?` all become `{}`, so only the shape compares. */
const shape = (path: string) =>
   path.replace(/\{\w+\}|:\w+|\*\??/g, "{}").replace(/^\/api\/v0/, "");

function specOperations(apiDoc: string): string[] {
   const operations: string[] = [];
   let path = "";
   for (const line of apiDoc.split("\n")) {
      const pathKey = line.match(/^ {2}(\/\S*):\s*$/);
      if (pathKey) {
         path = pathKey[1];
         continue;
      }
      if (/^\S/.test(line)) path = "";
      const verb = line.match(/^ {4}(get|post|put|patch|delete):\s*$/);
      if (verb && path)
         operations.push(`${verb[1].toUpperCase()} ${shape(path)}`);
   }
   return operations.sort();
}

describe("REST route parity with api-doc.yaml", () => {
   const apiDoc = readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8");
   const served = registeredRoutes(buildRouteApp())
      .filter((route) => !route.path.startsWith("/api/v0/projects"))
      .map((route) => `${route.method} ${shape(route.path)}`)
      .sort();

   it("serves exactly the operations the spec declares", () => {
      expect(served).toEqual(specOperations(apiDoc));
   });

   it("declares the data-app listing exactly once", () => {
      expect(
         specOperations(apiDoc).filter((op) => op.endsWith("/data-apps")),
      ).toEqual(["GET /environments/{}/packages/{}/data-apps"]);
   });

   it("no /pages listing path or operationId survives in the spec", () => {
      expect(apiDoc).not.toContain("/pages:");
      expect(apiDoc).not.toContain("operationId: list-pages");
   });
});

describe("admin route parity with admin-api-doc.yaml", () => {
   const adminDoc = readFileSync(
      resolve(REPO_ROOT, "admin-api-doc.yaml"),
      "utf8",
   );
   const served = registeredRoutes(createAdminApp(null))
      .filter((route) => route.path.startsWith("/api/v0/"))
      .map((route) => `${route.method} ${shape(route.path)}`)
      .sort();

   it("serves exactly the operations the admin spec declares", () => {
      expect(served).toEqual(specOperations(adminDoc));
   });

   it("names no main-API resource in an admin path", () => {
      for (const op of served) {
         expect(op).not.toMatch(
            /environments|packages|connections|storageDestinations/,
         );
      }
   });
});
