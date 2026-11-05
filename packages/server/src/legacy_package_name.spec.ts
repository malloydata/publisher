// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";
import {
   legacyPackageNameMiddleware,
   recoverVersionFromName,
   splitLegacyPackageName,
} from "./legacy_package_name";
import { API_PREFIX, buildRouteApp } from "./test_helpers/route_app";

// A combined package name, `<package>___<version>`, from an orchestrator that
// predates package versions: read as the package at that version when
// versioning is on, through every route, and left alone when it is off.

const ENV = `${API_PREFIX}/environments/acme`;

let saved: string | undefined;
beforeEach(() => {
   saved = process.env.PUBLISHER_PACKAGE_VERSIONING;
   process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
});
afterEach(() => {
   if (saved === undefined) delete process.env.PUBLISHER_PACKAGE_VERSIONING;
   else process.env.PUBLISHER_PACKAGE_VERSIONING = saved;
});

describe("splitLegacyPackageName", () => {
   it("splits at the last delimiter, when the suffix is a version", () => {
      expect(splitLegacyPackageName("sales___1.0.0")).toEqual({
         packageName: "sales",
         versionId: "1.0.0",
      });
      expect(splitLegacyPackageName("odd___name___2.0.0-rc.1+b7")).toEqual({
         packageName: "odd___name",
         versionId: "2.0.0-rc.1+b7",
      });
   });

   it("is no combined name without a version after the delimiter", () => {
      expect(splitLegacyPackageName("sales")).toBeUndefined();
      expect(splitLegacyPackageName("odd___name")).toBeUndefined();
      expect(splitLegacyPackageName("sales___1..0")).toBeUndefined();
      expect(splitLegacyPackageName("___1.0.0")).toBeUndefined();
      expect(splitLegacyPackageName(undefined)).toBeUndefined();
   });
});

describe("recoverVersionFromName", () => {
   it("reads the version from the name when the request names none", () => {
      for (const named of [undefined, null, "", "latest"]) {
         expect(recoverVersionFromName("sales___1.0.0", named)).toEqual({
            packageName: "sales",
            versionId: "1.0.0",
            recovered: true,
         });
      }
   });

   it("accepts the same version named both ways", () => {
      expect(recoverVersionFromName("sales___1.0.0", "1.0.0").recovered).toBe(
         true,
      );
   });

   it("leaves a different named version for the route to refuse", () => {
      expect(recoverVersionFromName("sales___1.0.0", "2.0.0")).toEqual({
         packageName: "sales___1.0.0",
         versionId: "2.0.0",
         recovered: false,
      });
   });

   it("does nothing with versioning off: the combined name is the package", () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      expect(recoverVersionFromName("sales___1.0.0", "").recovered).toBe(false);
   });
});

describe("routes reached with a combined package name", () => {
   it("reads a package at the version in its name", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         packageController: {
            getPackage: async (...args: unknown[]) => {
               calls.push(args);
               return {} as never;
            },
         },
      });
      await request(app)
         .get(`${ENV}/packages/sales___1.0.0?versionId=`)
         .expect(200);
      expect(calls).toEqual([["acme", "sales", false, "1.0.0"]]);
   });

   it("reads a model at that version", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         modelController: {
            getModel: async (...args: unknown[]) => {
               calls.push(args);
               return {} as never;
            },
         },
      });
      await request(app)
         .get(`${ENV}/packages/sales___1.0.0/models/flights.malloy`)
         .expect(200);
      expect(calls[0].slice(0, 3)).toEqual(["acme", "sales", "flights.malloy"]);
      expect(calls[0][3]).toMatchObject({ versionId: "1.0.0" });
   });

   it("runs a query at that version when the body names none", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         queryController: {
            getQuery: async (...args: unknown[]) => {
               calls.push(args);
               return { result: "[]" } as never;
            },
         },
      });
      await request(app)
         .post(`${ENV}/packages/sales___1.0.0/models/flights.malloy/query`)
         .send({ query: "run: flights -> by_carrier", versionId: null })
         .expect(200);
      expect(calls[0].slice(0, 2)).toEqual(["acme", "sales"]);
      expect(calls[0][10]).toMatchObject({ versionId: "1.0.0" });
   });

   it("builds that version when the body names none", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         materializationController: {
            createMaterialization: async (...args: unknown[]) => {
               calls.push(args);
               return {} as never;
            },
         },
      });
      await request(app)
         .post(`${ENV}/packages/sales___1.0.0/materializations`)
         .send({ forceRefresh: true })
         .expect(201);
      expect(calls).toEqual([
         ["acme", "sales", { forceRefresh: true, versionId: "1.0.0" }],
      ]);
   });

   it("loads a package named by a combined name as the package", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         packageController: {
            addPackage: async (...args: unknown[]) => {
               calls.push(args);
               return undefined as never;
            },
         },
      });
      await request(app)
         .post(`${ENV}/packages`)
         .send({ name: "sales___1.0.0", location: "gs://b/sales.zip" })
         .expect(200);
      expect(calls).toEqual([
         ["acme", { name: "sales", location: "gs://b/sales.zip" }],
      ]);
   });

   it("deletes only the version a combined name names", async () => {
      const deleted: unknown[][] = [];
      const app = buildRouteApp({
         versionController: {
            deleteVersion: async (...args: unknown[]) => {
               deleted.push(args);
               return {} as never;
            },
         },
         packageController: {
            deletePackage: async () => {
               throw new Error("the whole package must not be deleted");
            },
         },
      });
      await request(app).delete(`${ENV}/packages/sales___1.0.0`).expect(200);
      expect(deleted).toEqual([["acme", "sales", "1.0.0"]]);
   });

   it("rebinds that version's manifest for a PATCH of the combined name", async () => {
      const rebound: unknown[][] = [];
      const app = buildRouteApp({
         versionController: {
            setManifest: async (...args: unknown[]) => {
               rebound.push(args);
               return {} as never;
            },
         },
      });
      const body = {
         name: "sales___1.0.0",
         location: "gs://b/sales.zip",
         manifestLocation: "gs://b/manifest.json",
         queryMetadata: {},
      };
      await request(app)
         .patch(`${ENV}/packages/sales___1.0.0`)
         .send(body)
         .expect(200);
      expect(rebound).toEqual([
         ["acme", "sales", "1.0.0", { ...body, versionId: "1.0.0" }],
      ]);
   });

   it("leaves every route alone with versioning off", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         packageController: {
            deletePackage: async (...args: unknown[]) => {
               calls.push(args);
            },
         },
      });
      await request(app).delete(`${ENV}/packages/sales___1.0.0`).expect(200);
      expect(calls).toEqual([["acme", "sales___1.0.0"]]);
   });
});

describe("the legacy /projects routes reached with a combined package name", () => {
   const PROJECT = `${API_PREFIX}/projects/acme`;

   it("reads a package at the version in its name", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         packageController: {
            getPackage: async (...args: unknown[]) => {
               calls.push(args);
               return {} as never;
            },
         },
      });
      await request(app).get(`${PROJECT}/packages/sales___1.0.0`).expect(200);
      expect(calls).toEqual([["acme", "sales", false, "1.0.0"]]);
   });

   it("runs a query at that version", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         queryController: {
            getQuery: async (...args: unknown[]) => {
               calls.push(args);
               return { result: "[]" } as never;
            },
         },
      });
      await request(app)
         .post(`${PROJECT}/packages/sales___1.0.0/models/flights.malloy/query`)
         .send({ query: "run: flights -> by_carrier" })
         .expect(200);
      expect(calls[0].slice(0, 2)).toEqual(["acme", "sales"]);
      expect(calls[0][10]).toMatchObject({
         versionId: "1.0.0",
      });
   });

   it("builds that version", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         materializationController: {
            createMaterialization: async (...args: unknown[]) => {
               calls.push(args);
               return {} as never;
            },
         },
      });
      await request(app)
         .post(`${PROJECT}/packages/sales___1.0.0/materializations`)
         .send({})
         .expect(201);
      expect(calls).toEqual([["acme", "sales", { versionId: "1.0.0" }]]);
   });

   it("loads a package named by a combined name as the package", async () => {
      const calls: unknown[][] = [];
      const app = buildRouteApp({
         packageController: {
            addPackage: async (...args: unknown[]) => {
               calls.push(args);
               return undefined as never;
            },
         },
      });
      await request(app)
         .post(`${PROJECT}/packages`)
         .send({ name: "sales___1.0.0", location: "gs://b/sales.zip" });
      expect(calls).toEqual([
         ["acme", { name: "sales", location: "gs://b/sales.zip" }],
      ]);
   });

   it("deletes only that version, through the environment's version route", async () => {
      const deleted: unknown[][] = [];
      const app = buildRouteApp({
         versionController: {
            deleteVersion: async (...args: unknown[]) => {
               deleted.push(args);
               return {} as never;
            },
         },
         packageController: {
            deletePackage: async () => {
               throw new Error("the whole package must not be deleted");
            },
         },
      });
      await request(app)
         .delete(`${PROJECT}/packages/sales___1.0.0`)
         .expect(200);
      expect(deleted).toEqual([["acme", "sales", "1.0.0"]]);
   });

   it("rebinds that version's manifest for a PATCH", async () => {
      const rebound: unknown[][] = [];
      const app = buildRouteApp({
         versionController: {
            setManifest: async (...args: unknown[]) => {
               rebound.push(args);
               return {} as never;
            },
         },
      });
      await request(app)
         .patch(`${PROJECT}/packages/sales___1.0.0`)
         .send({ manifestLocation: "gs://b/manifest.json" })
         .expect(200);
      expect(rebound).toEqual([
         [
            "acme",
            "sales",
            "1.0.0",
            { manifestLocation: "gs://b/manifest.json", versionId: "1.0.0" },
         ],
      ]);
   });
});

describe("the data-app static routes", () => {
   it("serve a page of a combined name at its version", async () => {
      const seen: { path: string; versionId: unknown }[] = [];
      const app = express();
      app.use(legacyPackageNameMiddleware());
      app.get(
         "/environments/:environmentName/packages/:packageName/*",
         (req, res) => {
            seen.push({ path: req.path, versionId: req.query.versionId });
            res.sendStatus(204);
         },
      );
      await request(app)
         .get(
            "/environments/acme/packages/sales___1.0.0/apps/q3.html?versionId=latest",
         )
         .expect(204);
      expect(seen).toEqual([
         {
            path: "/environments/acme/packages/sales/apps/q3.html",
            versionId: "1.0.0",
         },
      ]);
   });

   it("serve a page of a combined name whatever version a forwarded query names", async () => {
      const seen: unknown[] = [];
      const app = express();
      app.use(legacyPackageNameMiddleware());
      app.get(
         "/environments/:environmentName/packages/:packageName/*",
         (req, res) => {
            // In the URL too, not only in Express 4's parsed `req.query`.
            const inUrl = new URLSearchParams(req.url.split("?")[1] ?? "");
            seen.push([
               req.params.packageName,
               req.query.versionId,
               inUrl.get("versionId"),
            ]);
            res.sendStatus(204);
         },
      );
      for (const forwarded of ["1.0.3", "2.0.0", "not-a-version"]) {
         await request(app)
            .get(
               `/environments/acme/packages/sales___1.0.3/index.html?versionId=${forwarded}`,
            )
            .expect(204);
      }
      expect(seen).toEqual([
         ["sales", "1.0.3", "1.0.3"],
         ["sales", "1.0.3", "1.0.3"],
         ["sales", "1.0.3", "1.0.3"],
      ]);
   });
});
