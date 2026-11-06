// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The legacy `/projects` aliases read `versionId` as the `/environments`
 * routes do: each route that declares it hands it to the same controller, so a
 * pinned version answers from that version rather than 501, and omitted means
 * `latest`.
 */

import { describe, expect, it } from "bun:test";
import request from "supertest";
import { PackageVersionError } from "./errors";
import { buildRouteApp } from "./test_helpers/route_app";

const PKG = "/api/v0/projects/env/packages/pkg";

/** The real route table, with controllers that record what they were handed. */
function recordingApp() {
   const calls: Record<string, unknown[]> = {};
   const record =
      (name: string) =>
      async (...args: unknown[]) => {
         calls[name] = args;
         return {};
      };
   // The legacy notebook routes reach the package through the store.
   const environmentStore = {
      getEnvironment: async () => ({
         assertCanAdmitQuery: () => {},
         getPackage: async (
            _packageName: string,
            _reload: boolean,
            options: { versionId?: unknown },
         ) => {
            calls.storeGetPackage = [options.versionId];
            throw new PackageVersionError(
               "VERSION_NOT_FOUND",
               "No such version.",
            );
         },
      }),
   };
   const stubs = {
      environmentStore,
      packageController: {
         getPackage: record("getPackage"),
         listPackages: record("listPackages"),
      },
      modelController: {
         listModels: record("listModels"),
         getModel: record("getModel"),
      },
      queryController: { getQuery: record("getQuery") },
      databaseController: { listDatabases: record("listDatabases") },
   } as unknown as Parameters<typeof buildRouteApp>[0];
   return { app: buildRouteApp(stubs), calls };
}

type Versioned = { versionId?: unknown };

describe("legacy /projects routes and versionId", () => {
   it.each([
      ["a package", `${PKG}`, "getPackage", (a: unknown[]) => a[3], 200],
      [
         "its models",
         `${PKG}/models`,
         "listModels",
         (a: unknown[]) => (a[2] as Versioned).versionId,
         200,
      ],
      [
         "a model",
         `${PKG}/models/m.malloy`,
         "getModel",
         (a: unknown[]) => (a[3] as Versioned).versionId,
         200,
      ],
      [
         "its databases",
         `${PKG}/databases`,
         "listDatabases",
         (a: unknown[]) => a[2],
         200,
      ],
      // The store answers 404 for the version, as it would for one never
      // published: the version reached the lookup.
      [
         "its notebooks",
         `${PKG}/notebooks`,
         "storeGetPackage",
         (a: unknown[]) => a[0],
         404,
      ],
      [
         "a notebook",
         `${PKG}/notebooks/n.malloynb`,
         "storeGetPackage",
         (a: unknown[]) => a[0],
         404,
      ],
      [
         "a notebook cell",
         `${PKG}/notebooks/n.malloynb/cells/0`,
         "storeGetPackage",
         (a: unknown[]) => a[0],
         404,
      ],
   ] as const)(
      "GET %s hands the controller the pinned version",
      async (_, path, method, versionOf, status) => {
         const { app, calls } = recordingApp();
         const res = await request(app).get(`${path}?versionId=1.2.0`);
         expect(res.status).toBe(status);
         expect(versionOf(calls[method] ?? [])).toBe("1.2.0");
      },
   );

   it("reads a package with no versionId as latest", async () => {
      const { app, calls } = recordingApp();
      const res = await request(app).get(PKG);
      expect(res.status).toBe(200);
      expect(calls.getPackage?.[3]).toBeUndefined();
   });

   it.each([
      ["the body", `${PKG}/models/m.malloy/query`, { versionId: "1.2.0" }],
      ["the URL", `${PKG}/models/m.malloy/query?versionId=1.2.0`, {}],
   ])("runs a query against the version %s names", async (_, path, extra) => {
      const { app, calls } = recordingApp();
      const res = await request(app)
         .post(path)
         .send({ query: "run: x -> y", ...extra });
      expect(res.status).toBe(200);
      expect((calls.getQuery?.[10] as Versioned).versionId).toBe("1.2.0");
   });

   it("refuses a query whose body and URL name different versions", async () => {
      const { app, calls } = recordingApp();
      const res = await request(app)
         .post(`${PKG}/models/m.malloy/query?versionId=1.3.0`)
         .send({ query: "run: x -> y", versionId: "1.2.0" });
      expect(res.status).toBe(400);
      expect(calls.getQuery).toBeUndefined();
   });

   it("refuses a version on the package listing", async () => {
      const { app, calls } = recordingApp();
      const res = await request(app).get(
         "/api/v0/projects/env/packages?versionId=1.2.0",
      );
      expect(res.status).toBe(400);
      expect(calls.listPackages).toBeUndefined();
   });

   it.each([["latest"], [""]])(
      "lists packages when versionId is %p",
      async (versionId) => {
         const { app, calls } = recordingApp();
         const res = await request(app).get(
            `/api/v0/projects/env/packages?versionId=${versionId}`,
         );
         expect(res.status).toBe(200);
         expect(calls.listPackages).toEqual(["env"]);
      },
   );
});
