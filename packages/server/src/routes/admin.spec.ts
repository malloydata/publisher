// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type express from "express";
import request from "supertest";
import type { WatchModeController } from "../controller/watch-mode.controller";
import { EnvironmentNotFoundError } from "../errors";
import {
   type CatalogAdmin,
   CatalogAdminError,
   catalogAdminConfig,
   connectionStringAsRole,
} from "../service/catalog_admin";
import { settings } from "../settings";
import { buildRouteApp } from "../test_helpers/route_app";
import { withSettings } from "../test_helpers/settings_env";
import { createAdminApp } from "./admin";

/** The admin port the variables `env` give a server. */
const adminPort = (env: Record<string, string>) =>
   withSettings({ env }, () => settings.get("admin.port"));

/** The admin host the variables `env` give a server. */
const adminHost = (env: Record<string, string>) =>
   withSettings({ env }, () => settings.get("admin.host"));

/** The catalog configuration the variables `env` give a server. */
const catalogConfigFrom = (env: Record<string, string>) =>
   withSettings({ env }, () => catalogAdminConfig());

const catalogs = (over: Partial<CatalogAdmin>): CatalogAdmin =>
   over as unknown as CatalogAdmin;

const watched = { environmentName: "" };
const watch = {
   current: () => ({
      enabled: !!watched.environmentName,
      environmentName: watched.environmentName,
      watchingPath: watched.environmentName
         ? `/srv/${watched.environmentName}`
         : "",
   }),
   watch: async (name: string) => {
      if (name === "missing") {
         throw new EnvironmentNotFoundError(`Environment ${name} not found`);
      }
      watched.environmentName = name;
      return watch.current();
   },
   unwatch: async () => {
      watched.environmentName = "";
   },
   // The main API's deprecated handlers.
   getWatchStatus: async (_req: unknown, res: express.Response) => {
      res.json(watch.current());
   },
   startWatching: async () => undefined,
   stopWatchMode: async () => undefined,
} as unknown as WatchModeController & {
   current: () => { enabled: boolean };
};

describe("the admin listener", () => {
   it("is off unless a port is given, and refuses one that is not a port", () => {
      expect(adminPort({})).toBeUndefined();
      expect(adminPort({ PUBLISHER_ADMIN_PORT: " " })).toBeUndefined();
      expect(adminPort({ PUBLISHER_ADMIN_PORT: "4050" })).toBe(4050);
      expect(() => adminPort({ PUBLISHER_ADMIN_PORT: "fifty" })).toThrow(
         'admin.port from PUBLISHER_ADMIN_PORT: expected an integer from 0 to 65535, got "fifty"',
      );
   });

   it("binds loopback unless told otherwise, and does not follow the main API's host", () => {
      expect(adminHost({})).toBe("127.0.0.1");
      expect(adminHost({ PUBLISHER_HOST: "0.0.0.0" })).toBe("127.0.0.1");
      expect(adminHost({ PUBLISHER_ADMIN_HOST: "0.0.0.0" })).toBe("0.0.0.0");
   });

   it("serves /metrics only when handed the handler, as an admin-only process is", async () => {
      expect(
         (await request(createAdminApp(null, watch)).get("/metrics")).status,
      ).toBe(404);
      const app = createAdminApp(null, null, (_req, res) => {
         res.type("text/plain").send("publisher_up 1\n");
      });
      const res = await request(app).get("/metrics");
      expect([res.status, res.text]).toEqual([200, "publisher_up 1\n"]);
   });

   it("answers 503 on every catalog route when no catalog database is configured", async () => {
      const res = await request(createAdminApp(null, watch)).get(
         "/api/v0/catalogs",
      );
      expect(res.status).toBe(503);
      expect(res.body.reason).toBe("CATALOG_DATABASE_NOT_CONFIGURED");
   });

   it("answers an admin refusal with its status and reason", async () => {
      const app = createAdminApp(
         catalogs({
            getCatalog: async () => {
               throw new CatalogAdminError(404, "CATALOG_NOT_FOUND", "no");
            },
         }),
         watch,
      );
      const res = await request(app).get("/api/v0/catalogs/ds_x");
      expect(res.status).toBe(404);
      expect(res.body).toEqual({
         code: 404,
         message: "no",
         reason: "CATALOG_NOT_FOUND",
      });
   });

   it("creates with 201, and a PUT answers 201 for a new table and 200 for a replacement", async () => {
      let created = true;
      let ifAbsent: boolean | undefined;
      const app = createAdminApp(
         catalogs({
            createCatalog: async () => ({
               name: "ds_x",
               dataPath: "/d",
               encrypted: true,
               formatVersion: "1.0",
               sizeBytes: 0,
            }),
            putTable: async (_c, _s, _t, _b, absent) => {
               ifAbsent = absent;
               return {
                  table: { schema: "main", name: "t", columns: [] },
                  created,
               };
            },
         }),
         watch,
      );
      expect(
         (await request(app).post("/api/v0/catalogs").send({})).status,
      ).toBe(201);
      const path = "/api/v0/catalogs/ds_x/schemas/main/tables/t";
      expect((await request(app).put(path).send({})).status).toBe(201);
      expect(ifAbsent).toBe(false);
      created = false;
      expect(
         (await request(app).put(path).set("If-None-Match", "*").send({}))
            .status,
      ).toBe(200);
      expect(ifAbsent).toBe(true);
   });

   it("refuses a snapshot delete without a valid olderThan", async () => {
      const app = createAdminApp(catalogs({}), watch);
      const res = await request(app).delete("/api/v0/catalogs/ds_x/snapshots");
      expect(res.status).toBe(400);
      expect(res.body.reason).toBe("INVALID_REQUEST");
   });

   it("serves watch mode as one resource: read, set, turn off", async () => {
      const app = createAdminApp(null, watch);
      const off = await request(app).get("/api/v0/watch-mode");
      expect([off.status, off.body.enabled]).toEqual([200, false]);
      const on = await request(app)
         .put("/api/v0/watch-mode")
         .send({ environmentName: "local" });
      expect(on.status).toBe(200);
      expect(on.body).toMatchObject({
         enabled: true,
         environmentName: "local",
      });
      expect(
         (await request(app).put("/api/v0/watch-mode").send({})).status,
      ).toBe(400);
      expect(
         (
            await request(app)
               .put("/api/v0/watch-mode")
               .send({ environmentName: "missing" })
         ).status,
      ).toBe(404);
      expect((await request(app).delete("/api/v0/watch-mode")).status).toBe(
         204,
      );
      expect((await request(app).get("/api/v0/watch-mode")).body.enabled).toBe(
         false,
      );
   });

   it("still answers the main API's deprecated watch-mode status, marked", async () => {
      const main = await request(
         buildRouteApp({ watchModeController: watch }),
      ).get("/api/v0/watch-mode/status");
      expect(main.status).toBe(200);
      expect(main.headers.deprecation).toBe("true");
   });

   it("still serves the main API's deprecated storage routes, marked", async () => {
      const app = buildRouteApp({
         storageDestinationController: {
            listTables: async () => [{ name: "t" }],
         },
      });
      const listed = await request(app).get(
         "/api/v0/environments/e/storageDestinations/lake/schemas/main/tables",
      );
      expect(listed.status).toBe(200);
      expect(listed.headers.deprecation).toBe("true");
   });
});

describe("the admin listener's configuration", () => {
   it("is null without a catalog database, and reads each storage provider", () => {
      expect(catalogConfigFrom({})).toBeNull();
      const db = "host=h dbname=ducklake user=a password=p";
      expect(
         catalogConfigFrom({ PUBLISHER_ADMIN_CATALOG_DATABASE: db }),
      ).toEqual({ catalogDatabase: db, storage: undefined });
      expect(
         catalogConfigFrom({
            PUBLISHER_ADMIN_CATALOG_DATABASE: db,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "gcs",
            PUBLISHER_ADMIN_STORAGE_KEY_ID: "k",
            PUBLISHER_ADMIN_STORAGE_SECRET: "s",
         })?.storage,
      ).toEqual({ gcsConnection: { keyId: "k", secret: "s" } });
      expect(
         catalogConfigFrom({
            PUBLISHER_ADMIN_CATALOG_DATABASE: db,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3_credential_chain",
            PUBLISHER_ADMIN_STORAGE_REGION: "us-east-1",
         })?.storage,
      ).toEqual({
         s3Connection: { provider: "credential_chain", region: "us-east-1" },
      });
   });

   it("fails startup on a provider it cannot use", () => {
      const db = "host=h";
      expect(() =>
         catalogConfigFrom({
            PUBLISHER_ADMIN_CATALOG_DATABASE: db,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "azure",
         }),
      ).toThrow(
         'admin.storage.provider from PUBLISHER_ADMIN_STORAGE_PROVIDER: expected one of "s3", "s3_credential_chain" or "gcs", got "azure"',
      );
      expect(() =>
         catalogConfigFrom({
            PUBLISHER_ADMIN_CATALOG_DATABASE: db,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3",
         }),
      ).toThrow(/needs PUBLISHER_ADMIN_STORAGE_KEY_ID/);
   });

   it("acts as a role through libpq options, in either connection-string form", () => {
      expect(connectionStringAsRole("host=h user=a", "ds_x")).toBe(
         "host=h user=a options=-crole=ds_x",
      );
      expect(connectionStringAsRole("postgres://a@h/db", "ds_x")).toBe(
         "postgres://a@h/db?options=-crole%3Dds_x",
      );
      expect(
         connectionStringAsRole("postgres://a@h/db?sslmode=require", "ds_x"),
      ).toBe("postgres://a@h/db?sslmode=require&options=-crole%3Dds_x");
      expect(connectionStringAsRole("host=h", undefined)).toBe("host=h");
   });

   it("refuses to act as an owner whose name would re-point the connection", () => {
      for (const hostile of [
         "x host=evil.example",
         "x options=-cfoo",
         "a'b",
         "a b",
         "a&host=evil",
      ]) {
         expect(() => connectionStringAsRole("host=h user=a", hostile)).toThrow(
            CatalogAdminError,
         );
      }
      expect(connectionStringAsRole("host=h", "x-ducklake")).toBe(
         "host=h options=-crole=x-ducklake",
      );
   });

   it("refuses a configured connection string that sets its own options", () => {
      for (const db of [
         "host=h options=-cstatement_timeout=5s",
         "postgres://a@h/db?options=-cfoo",
      ]) {
         expect(() =>
            catalogConfigFrom({ PUBLISHER_ADMIN_CATALOG_DATABASE: db }),
         ).toThrow(/must not set `options`/);
      }
   });
});
