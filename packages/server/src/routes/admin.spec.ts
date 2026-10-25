// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import request from "supertest";
import type { StorageDestinationController } from "../controller/storage_destination.controller";
import { buildRouteApp } from "../test_helpers/route_app";
import { createAdminApp, resolveAdminHost, resolveAdminPort } from "./admin";

const TABLES =
   "/api/v0/environments/e/storageDestinations/lake/schemas/main/tables";

const controller = {
   listTables: async () => [{ name: "t" }],
   createBootstrap: async () => ({ encrypted: true, schemas: ["main"] }),
} as unknown as StorageDestinationController;

describe("the admin listener", () => {
   it("is off unless a port is given, and refuses one that is not a port", () => {
      expect(resolveAdminPort({})).toBeNull();
      expect(resolveAdminPort({ PUBLISHER_ADMIN_PORT: " " })).toBeNull();
      expect(resolveAdminPort({ PUBLISHER_ADMIN_PORT: "4050" })).toBe(4050);
      expect(() => resolveAdminPort({ PUBLISHER_ADMIN_PORT: "fifty" })).toThrow(
         /must be a port number/,
      );
   });

   it("binds loopback unless told otherwise, and does not follow the main API's host", () => {
      expect(resolveAdminHost({})).toBe("127.0.0.1");
      expect(resolveAdminHost({ PUBLISHER_HOST: "0.0.0.0" })).toBe("127.0.0.1");
      expect(resolveAdminHost({ PUBLISHER_ADMIN_HOST: "0.0.0.0" })).toBe(
         "0.0.0.0",
      );
   });

   it("serves the catalog operations, with no Deprecation header", async () => {
      const app = createAdminApp(controller);
      const listed = await request(app).get(TABLES);
      expect(listed.status).toBe(200);
      expect(listed.body).toEqual([{ name: "t" }]);
      expect(listed.headers.deprecation).toBeUndefined();
      const boot = await request(app)
         .post("/api/v0/environments/e/storageDestinations/lake/bootstraps")
         .send({});
      expect(boot.status).toBe(201);
   });

   it("leaves the main API the deprecated three, marked, and not the new two", async () => {
      const app = buildRouteApp({ storageDestinationController: controller });
      const listed = await request(app).get(TABLES);
      expect(listed.status).toBe(200);
      expect(listed.headers.deprecation).toBe("true");
      const boot = await request(app)
         .post("/api/v0/environments/e/storageDestinations/lake/bootstraps")
         .send({});
      expect(boot.status).toBe(404);
   });
});
