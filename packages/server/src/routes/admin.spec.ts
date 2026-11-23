// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
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
                  table: {
                     schema: "s",
                     name: "t",
                     columns: [],
                     partitionedBy: [],
                  },
                  created,
               };
            },
         }),
         watch,
      );
      expect(
         (await request(app).post("/api/v0/catalogs").send({})).status,
      ).toBe(201);
      const path = "/api/v0/catalogs/ds_x/schemas/s/tables/t";
      expect((await request(app).put(path).send({})).status).toBe(201);
      expect(ifAbsent).toBe(false);
      created = false;
      expect(
         (await request(app).put(path).set("If-None-Match", "*").send({}))
            .status,
      ).toBe(200);
      expect(ifAbsent).toBe(true);
   });

   it("routes a load POST with the body and Idempotency-Key as given: 201 with a Location for a created load, 200 with none for a no-op, and a refusal's detail", async () => {
      const seen: unknown[] = [];
      let refuse: CatalogAdminError | undefined = undefined;
      let created = true;
      const counts = {
         mode: "merge" as const,
         rowsInserted: 1,
         rowsReplaced: 0,
         rowsUnchanged: 0,
         rowsDeleted: 0,
         rowCount: 1,
         cursorMax: null,
      };
      const app = createAdminApp(
         catalogs({
            loadTable: async (c, s, t, body, key) => {
               seen.push([c, s, t, body, key]);
               if (refuse) throw refuse;
               return {
                  load: created
                     ? { ...counts, id: "abc", snapshotId: 7, committedAt: "x" }
                     : counts,
                  created,
               };
            },
         }),
         watch,
      );
      const path = "/api/v0/catalogs/ds_x/schemas/s/tables/t/loads";
      const body = {
         commitMessage: "nightly",
         mode: "merge",
         key: ["id"],
         objects: ["o"],
      };
      const ok = await request(app)
         .post(path)
         .set("Idempotency-Key", "run-1")
         .send(body);
      expect(ok.status).toBe(201);
      expect(ok.headers.location).toBe("loads/abc");
      expect(ok.body).toMatchObject({ id: "abc", rowsInserted: 1 });
      expect(seen).toEqual([["ds_x", "s", "t", body, "run-1"]]);
      created = false;
      const noop = await request(app).post(path).send(body);
      expect(noop.status).toBe(200);
      expect(noop.headers.location).toBeUndefined();
      expect(noop.body).not.toHaveProperty("id");
      expect(seen[1]).toEqual(["ds_x", "s", "t", body, undefined]);
      refuse = new CatalogAdminError(
         422,
         "LOAD_DELETE_LIMIT_EXCEEDED",
         "too many",
         {
            wouldDelete: 4,
            rowsBefore: 5,
            maxDeletedRows: 3,
         },
      );
      const refused = await request(app).post(path).send(body);
      expect(refused.status).toBe(422);
      expect(refused.body).toEqual({
         code: 422,
         message: "too many",
         reason: "LOAD_DELETE_LIMIT_EXCEEDED",
         detail: { wouldDelete: 4, rowsBefore: 5, maxDeletedRows: 3 },
      });
   });

   it("routes a load's reads: one by id, and a list with its limit and cursor, refusing a limit outside 1 to 1000", async () => {
      const seen: unknown[] = [];
      const read = { id: "abc", mode: "merge", author: "publisher-admin" };
      const app = createAdminApp(
         catalogs({
            getLoad: async (c, s, t, id) => {
               seen.push(["get", c, s, t, id]);
               if (id === "gone") {
                  throw new CatalogAdminError(404, "LOAD_NOT_FOUND", "no");
               }
               return read as never;
            },
            listLoads: async (c, s, t, limit, after) => {
               seen.push(["list", c, s, t, limit, after]);
               return { items: [read as never], nextCursor: "next" };
            },
         }),
         watch,
      );
      const path = "/api/v0/catalogs/ds_x/schemas/s/tables/t/loads";
      expect((await request(app).get(`${path}/abc`)).body).toEqual(read);
      const gone = await request(app).get(`${path}/gone`);
      expect([gone.status, gone.body.reason]).toEqual([404, "LOAD_NOT_FOUND"]);
      const list = await request(app).get(`${path}?limit=5&after=MQ`);
      expect(list.body).toEqual({ items: [read], nextCursor: "next" });
      await request(app).get(path);
      expect(seen).toEqual([
         ["get", "ds_x", "s", "t", "abc"],
         ["get", "ds_x", "s", "t", "gone"],
         ["list", "ds_x", "s", "t", 5, "MQ"],
         ["list", "ds_x", "s", "t", 100, undefined],
      ]);
      for (const limit of ["0", "1001", "1.5", "x"]) {
         const refused = await request(app).get(`${path}?limit=${limit}`);
         expect([limit, refused.status, refused.body.reason]).toEqual([
            limit,
            400,
            "INVALID_REQUEST",
         ]);
      }
   });

   it("routes a schema PUT to 201 or 200, passes If-None-Match, and maps a refusal to its reason", async () => {
      let created = true;
      const seen: unknown[][] = [];
      const app = createAdminApp(
         catalogs({
            createSchema: async (...args: unknown[]) => {
               seen.push(args);
               if (args[1] === "taken") {
                  throw new CatalogAdminError(412, "SCHEMA_EXISTS", "exists");
               }
               if (args[1] === "main") {
                  throw new CatalogAdminError(400, "SCHEMA_RESERVED", "no");
               }
               return { schema: { name: args[1] as string }, created };
            },
         } as unknown as Partial<CatalogAdmin>),
         watch,
      );
      const path = (s: string) => `/api/v0/catalogs/ds_x/schemas/${s}`;
      const made = await request(app).put(path("staging"));
      expect([made.status, made.body]).toEqual([201, { name: "staging" }]);
      expect(seen[0]).toEqual(["ds_x", "staging", {}, false]);
      created = false;
      expect((await request(app).put(path("staging"))).status).toBe(200);
      await request(app).put(path("staging")).set("If-None-Match", "*");
      expect(seen[2][3]).toBe(true);
      const taken = await request(app)
         .put(path("taken"))
         .set("If-None-Match", "*");
      expect([taken.status, taken.body.reason]).toEqual([412, "SCHEMA_EXISTS"]);
      const reserved = await request(app).put(path("main"));
      expect([reserved.status, reserved.body.reason]).toEqual([
         400,
         "SCHEMA_RESERVED",
      ]);
   });

   it("routes a schema DELETE to 204, and maps a refusal to its reason", async () => {
      const dropped: string[][] = [];
      const app = createAdminApp(
         catalogs({
            deleteSchema: async (c: string, s: string) => {
               if (s === "main") {
                  throw new CatalogAdminError(400, "SCHEMA_RESERVED", "no");
               }
               dropped.push([c, s]);
            },
         }),
         watch,
      );
      const res = await request(app).delete(
         "/api/v0/catalogs/ds_x/schemas/old",
      );
      expect([res.status, res.text, dropped]).toEqual([
         204,
         "",
         [["ds_x", "old"]],
      ]);
      const reserved = await request(app).delete(
         "/api/v0/catalogs/ds_x/schemas/main",
      );
      expect([reserved.status, reserved.body.reason]).toEqual([
         400,
         "SCHEMA_RESERVED",
      ]);
   });

   it("lists a catalog's tables across schemas apart from one schema's", async () => {
      const app = createAdminApp(
         catalogs({
            listTables: (async (_c: string, schema?: string) =>
               schema === undefined
                  ? [{ schema: "main", name: "t", sizeBytes: 1 }]
                  : [
                       { name: "t", sizeBytes: 1 },
                    ]) as CatalogAdmin["listTables"],
         }),
         watch,
      );
      expect(
         (await request(app).get("/api/v0/catalogs/ds_x/tables")).body,
      ).toEqual([{ schema: "main", name: "t", sizeBytes: 1 }]);
      expect(
         (await request(app).get("/api/v0/catalogs/ds_x/schemas/main/tables"))
            .body,
      ).toEqual([{ name: "t", sizeBytes: 1 }]);
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

   it("fails startup on an S3 endpoint it cannot use, without repeating it", () => {
      const env = {
         PUBLISHER_ADMIN_CATALOG_DATABASE: "host=h",
         PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3",
         PUBLISHER_ADMIN_STORAGE_KEY_ID: "k",
         PUBLISHER_ADMIN_STORAGE_SECRET: "s",
      };
      expect(() =>
         catalogConfigFrom({
            ...env,
            PUBLISHER_ADMIN_STORAGE_ENDPOINT: "https://key:hunter2@minio",
         }),
      ).toThrow(
         "PUBLISHER_ADMIN_STORAGE_ENDPOINT: S3 endpoint must not contain userinfo",
      );
      expect(
         catalogConfigFrom({
            ...env,
            PUBLISHER_ADMIN_STORAGE_ENDPOINT: "http://minio:9000",
         })?.storage?.s3Connection?.endpoint,
      ).toBe("http://minio:9000");
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

   it("fails startup on a connection string the load lock cannot use, without repeating its password", () => {
      for (const db of [
         "service=prod password=hunter2",
         "host=a,b password=hunter2",
         "host=h hostaddr=10.0.0.1 password=hunter2",
      ]) {
         const error = (() => {
            try {
               catalogConfigFrom({ PUBLISHER_ADMIN_CATALOG_DATABASE: db });
            } catch (e) {
               return String(e);
            }
            return "";
         })();
         expect(error).toContain(
            "PUBLISHER_ADMIN_CATALOG_DATABASE: the connection string sets",
         );
         expect(error).not.toContain("hunter2");
      }
   });
});
